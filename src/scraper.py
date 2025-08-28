import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import pandas as pd
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

RRC_URL = "https://webapps2.rrc.texas.gov/EWA/drillingPermitsQueryAction.do"

def get_county_codes():
    return {
        "ANDREWS": "003", "ECTOR": "135", "MIDLAND": "329", "MARTIN": "317",
        "HOWARD": "227", "GLASSCOCK": "173", "REAGAN": "383", "UPTON": "461",
        "CRANE": "103", "WARD": "475", "WINKLER": "495", "LOVING": "301",
        "REEVES": "389", "PECOS": "371"
    }

def parse_results_table(soup):
    permits_on_page = []
    results_table = soup.find('table', {'class': 'DataGrid'})
    if not results_table:
        # logging.warning("No results table found on the page.")
        return []

    # Use recursive=False to get only the direct children tr elements
    table_body = results_table.find('tbody')
    if not table_body:
        # logging.warning("No tbody found in the results table.")
        return []
    rows = table_body.find_all('tr', recursive=False)

    if len(rows) < 3:
        # logging.info("No data rows found in the table.")
        return []

    # The header is the second row of the main table
    header_row = rows[1]
    headers = [th.text.strip() for th in header_row.find_all('th')]

    # The first column is complex, we'll name it 'API_NO' and handle it separately
    headers[0] = 'API NO.'
    headers.insert(1, 'PlatLink')

    # Data rows start from the third row
    data_rows = rows[2:]
    for row in data_rows:
        cols = row.find_all('td')
        if not cols:
            continue

        permit_data = {}

        api_no_tag = cols[0].find('a')
        permit_data['API NO.'] = api_no_tag.text.strip() if api_no_tag else ''

        plat_link = ''
        links_select = cols[0].find('select')
        if links_select:
            for option in links_select.find_all('option'):
                if 'Images' in option.text:
                    try:
                        value_json = json.loads(option['value'])
                        plat_link = value_json.get('url', '')
                    except (json.JSONDecodeError, KeyError):
                         logging.warning(f"Could not parse plat link for API {permit_data['API NO.']}")
        permit_data['PlatLink'] = plat_link

        for i, header in enumerate(headers[2:], start=1):
            text_content = cols[i].text.strip().replace('\n', ' ').replace('\r', '').replace(u'\xa0', ' ').strip()
            permit_data[header] = ' '.join(text_content.split())

        permits_on_page.append(permit_data)

    return permits_on_page


async def scrape_permits(config):
    # logging.info("Starting permit scraping process with Playwright.")

    county_codes_map = get_county_codes()
    selected_county_codes = [county_codes_map[county] for county in config['counties'] if county in county_codes_map]

    if not selected_county_codes:
        # logging.error("No valid counties selected in the config.")
        return pd.DataFrame()

    all_permits_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            # logging.info(f"Navigating to {RRC_URL}")
            await page.goto(RRC_URL, timeout=60000)

            await page.select_option('select[name="searchArgs.countyCodeHndlr.selectedCodes"]', selected_county_codes)
            await page.fill('input[name="searchArgs.approvedDtFromHndlr.inputValue"]', config['date_range']['from'])
            await page.fill('input[name="searchArgs.approvedDtToHndlr.inputValue"]', config['date_range']['to'])

            # logging.info("Submitting the form.")
            await page.click('input[type="submit"][value="Submit"]')

            await page.wait_for_selector('table.DataGrid', state='visible', timeout=30000)
            # logging.info("Results page loaded.")

            page_num = 1
            while True:
                logging.info(f"Scraping page {page_num}...")
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')

                permits_on_page = parse_results_table(soup)
                if not permits_on_page:
                    # logging.info("No more permits found on this page. Ending scrape.")
                    break

                all_permits_data.extend(permits_on_page)
                # logging.info(f"Scraped {len(permits_on_page)} permits from page {page_num}.")

                next_link = page.locator('a:has-text("[Next>]")')
                if await next_link.count() > 0:
                    # logging.info("Found 'Next' link, clicking to go to the next page.")
                    await next_link.click()
                    await page.wait_for_load_state('networkidle')
                    page_num += 1
                else:
                    # logging.info("No 'Next' link found. This is the last page.")
                    break

        except Exception as e:
            # logging.error(f"An error occurred during the Playwright process: {e}")
            await page.screenshot(path='error_screenshot.png')
            # logging.info("A screenshot has been saved as 'error_screenshot.png'")
        finally:
            await browser.close()

    # logging.info(f"Scraping complete. Total permits scraped: {len(all_permits_data)}")
    return pd.DataFrame(all_permits_data)

import httpx
import os

async def download_plat_files(df, client=None):
    """
    Downloads plat files from NeuDocs by navigating through Action menu -> Download Well Log -> tif_1.
    Handles slow pages, skips already downloaded files, and logs each step.
    """
    logging.info("Starting plat file download process (optimized for slow pages).")
    plat_file_paths = []
    timeout = 60000  # 60 seconds timeout for slow pages

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=500)  # Debug mode
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        for _, row in df.iterrows():
            plat_link = row.get('PlatLink')
            api_no = row.get('API NO.')
            county = row.get('County', 'UnknownCounty')

            if not plat_link or not api_no:
                logging.warning(f"No plat link for API {api_no}")
                plat_file_paths.append('')
                continue

            # Prepare file path
            download_dir = os.path.join('data', 'plat_files', county)
            os.makedirs(download_dir, exist_ok=True)
            file_path = os.path.join(download_dir, f"{api_no}.tif")

            # Skip if file already exists
            if os.path.exists(file_path):
                logging.info(f"File already exists for API {api_no}, skipping.")
                plat_file_paths.append(file_path)
                continue

            try:
                logging.info(f"[API {api_no}] Opening plat link...")
                await page.goto(plat_link, timeout=timeout)

                # Wait for and click the "Action" menu button
                logging.info(f"[API {api_no}] Waiting for Action menu...")
                await page.locator(".showActionMenu").first.wait_for(state="visible", timeout=timeout)
                # Get all record numbers first
                record_numbers = await page.locator(".showActionMenu").evaluate_all(
                    "elements => elements.map(el => el.getAttribute('recordnumber')).filter(n => n !== null)"
                )

                logging.info(f"[API {api_no}] Found recordnumbers: {record_numbers}")

                for record_num in record_numbers:
                    logging.info(f"[API {api_no}] Processing recordnumber={record_num}")

                    # Locate by recordnumber attribute directly
                    btn = page.locator(f'.showActionMenu[recordnumber="{record_num}"]')
                    await btn.scroll_into_view_if_needed()
                    await btn.wait_for(state="visible", timeout=timeout)
                    await btn.click()

                    # Wait for context menu
                    await page.locator("#imageMenu").wait_for(state="visible", timeout=timeout)

                    # Click "Download Well Log"
                    await page.locator('#imageMenu a:has-text("Download Well Log")').click(timeout=timeout)

                    # Wait for download link
                    download_link = page.locator("a.image.download, a.download")
                    await download_link.wait_for(state="visible", timeout=timeout)

                    # Download file
                    filename = f"{api_no}_{record_num}.tif"
                    file_path = os.path.join(download_dir, filename)
                    async with page.expect_download() as download_info:
                        await download_link.click()
                    download = await download_info.value
                    await download.save_as(file_path)

                    logging.info(f"[API {api_no}] Saved file: {file_path}")

                    # Close the doc to go back
                    await page.locator("#closeDoc").click()

            except Exception as e:
                logging.error(f"[API {api_no}] Failed: {e}")
                plat_file_paths.append('')

        await browser.close()

    df['PlatFilePath'] = plat_file_paths
    return df

# The main block is removed as this script is now intended to be used as a module.
