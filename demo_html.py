from pathlib import Path

from custom_command import LinkCountingCommand
from openwpm.command_sequence import CommandSequence
from openwpm.commands.browser_commands import GetCommand
from openwpm.config import BrowserParams, ManagerParams
from openwpm.storage.sql_provider import SQLiteStorageProvider
from openwpm.storage.leveldb import LevelDbProvider
from openwpm.task_manager import TaskManager

import csv
from multiprocessing import Pool, Process

def read_url_list(fpath):
    first_elements_list = []
    print(fpath)
    with open(fpath, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row and len(row) > 1:
                first_elements_list.append("file://" + row[0])
    return first_elements_list

def crawling(crawler_idx, sites_all, start, end):
    # Loads the default ManagerParams
    # and NUM_BROWSERS copies of the default BrowserParams

    my_sites = sites_all[start:end]
    
    manager_params = ManagerParams(num_browsers=NUM_BROWSERS)
    browser_params = [BrowserParams(display_mode="headless") for _ in range(NUM_BROWSERS)]

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
    manager_params.data_directory = Path("./datadir_local/")
    manager_params.log_path = Path("./datadir_local/log_dir/openwpm_{}.log".format(crawler_idx))

    # memory_watchdog and process_watchdog are useful for large scale cloud crawls.
    # Please refer to docs/Configuration.md#platform-configuration-options for more information
    # manager_params.memory_watchdog = True
    # manager_params.process_watchdog = True

    # Commands time out by default after 60 seconds
    with TaskManager(
        manager_params,
        browser_params,
        SQLiteStorageProvider(Path("./datadir_local/crawl_dir/crawl-data_{}.sqlite".format(crawler_idx))),
        LevelDbProvider(Path("./datadir_local/content_dir/content_{}.ldb".format(crawler_idx))),
    ) as manager:
        # Visits the sites
        for index, site in enumerate(my_sites):

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
NUM_CORE = 80

BASE_DIR = "/data2/shine/a4_pipeline/ad_Nbackdoor"
TOP10K_FPATH_HTML = "/data2/shine/a4_pipeline/ad_Nbackdoor/rendering_stream/map_local_list_unmod_final_absolute.csv"

sites_all = read_url_list(TOP10K_FPATH_HTML)
    
processes = []
num_total = len(sites_all)
print(sites_all)

per_core = 200

for i in range(int(num_total / per_core + 1)):
    name = str(i)
    p = Process(target=crawling, args=(name, sites_all, i * per_core, (i + 1) * per_core,))
    p.start()
    processes.append(p)

for p in processes:
    p.join()