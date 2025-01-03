from pathlib import Path

from custom_command import LinkCountingCommand
from openwpm.command_sequence import CommandSequence
from openwpm.commands.browser_commands import GetCommand
from openwpm.config import BrowserParams, ManagerParams
from openwpm.storage.sql_provider import SQLiteStorageProvider
from openwpm.storage.leveldb import LevelDbProvider
from openwpm.task_manager import TaskManager
from selenium.webdriver.common.proxy import Proxy, ProxyType

import csv
from multiprocessing import Pool, Process

def read_url_list(fpath):
    url_list = []
    with open(fpath, 'r') as fin:
        reader = csv.DictReader(fin)
        for row in reader:
            url_list.append("http://" + row['DOMAIN'])
    return url_list

def crawling(crawler_idx, sites_all, port_num):
    # Loads the default ManagerParams
    # and NUM_BROWSERS copies of the default BrowserParams
    
    manager_params = ManagerParams(num_browsers=NUM_BROWSERS)
    # shine
    browser_params = [BrowserParams(display_mode="headless", port_num=port_num) for _ in range(NUM_BROWSERS)]

    # Update browser configuration (use this for per-browser settings)
    for browser_param in browser_params:
        # Record HTTP Requests and Responses
        browser_param.http_instrument = True
        # Record cookie changes
        browser_param.cookie_instrument = True
        # Record Navigations
        browser_param.navigation_instrument = True
        # Record JS Web API calls
        browser_param.js_instrument = True
        # Record the callstack of all WebRequests made
        browser_param.callstack_instrument = True
        # Record DNS resolution
        browser_param.dns_instrument = True
        # save the javascript files
        browser_param.save_content = "script"
        


    # Update TaskManager configuration (use this for crawl-wide settings)
    manager_params.data_directory = Path("/yopo-artifact/OpenWPM/datadir_proxy_unmod/")
    manager_params.log_path = Path("/yopo-artifact/OpenWPM/datadir_proxy_unmod/log_dir/openwpm_{}.log".format(crawler_idx))

    # memory_watchdog and process_watchdog are useful for large scale cloud crawls.
    # Please refer to docs/Configuration.md#platform-configuration-options for more information
    # manager_params.memory_watchdog = True
    # manager_params.process_watchdog = True

    # Commands time out by default after 60 seconds
    with TaskManager(
        manager_params,
        browser_params,
        SQLiteStorageProvider(Path("/yopo-artifact/OpenWPM/datadir_proxy_unmod/crawl_dir/crawl-data_{}.sqlite".format(crawler_idx))),
        LevelDbProvider(Path("/yopo-artifact/OpenWPM/datadir_proxy_unmod/content_dir/content_{}.ldb".format(crawler_idx))),
    ) as manager:
        # Visits the sites
        for index, site in enumerate(sites_all):
            if index % 16 == int(crawler_idx):
                pass
            else:
                continue

            def callback(success: bool, val: str = site) -> None:
                print(
                    f"CommandSequence for {val} ran {'successfully' if success else 'unsuccessfully'}"
                )

            # Parallelize sites over all number of browsers set above.
            command_sequence = CommandSequence(
                site,
                site_rank=index,
                callback=callback,
            )

            # Start by visiting the page
            command_sequence.append_command(GetCommand(url=site, sleep=3), timeout=60)
            # Have a look at custom_command.py to see how to implement your own command
            command_sequence.append_command(LinkCountingCommand())

            # Run commands across all browsers (simple parallelization)
            manager.execute_command_sequence(command_sequence)

# The list of sites that we wish to crawl
NUM_BROWSERS = 1
TOP10K_FPATH = "/yopo-artifact/data/rendering_stream/map_local_list_unmod_final.csv"
final_urls = []
with open(TOP10K_FPATH, 'r') as csvfile:
    # Create a CSV reader object
    csv_reader = csv.reader(csvfile)
    
    # Iterate over each row in the CSV file
    for row in csv_reader:
        # Ensure the row has at least two columns
        if len(row) >= 2:
            # Append the second column value to the list
            final_urls.append(row[1])


processes = []
num_total = len(final_urls)
base_port = 5555
num_core = 16

for i in range(num_core):   # 16
    name = str(i)
    p = Process(target=crawling, args=(name, final_urls, str(base_port + i)))
    p.start()
    processes.append(p)

for p in processes:
    p.join()