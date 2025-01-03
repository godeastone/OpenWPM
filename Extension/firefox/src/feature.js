/******/ (() => { // webpackBootstrap
/******/ 	"use strict";
/******/ 	var __webpack_modules__ = ({

/***/ "./feature.js/callstack-instrument.js":
/*!********************************************!*\
  !*** ./feature.js/callstack-instrument.js ***!
  \********************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "CallstackInstrument": () => (/* binding */ CallstackInstrument)
/* harmony export */ });
/*
  We capture the JS callstack when we detect a dynamically created http request
  and bubble it up via a WebExtension Experiment API stackDump.
  This instrumentation captures those and saves them to the "callstacks" table.
*/
class CallstackInstrument {
  constructor(dataReceiver) {
    this.dataReceiver = dataReceiver;
  }
  run(browser_id) {
    browser.stackDump.onStackAvailable.addListener((request_id, call_stack) => {
      const record = {
        browser_id,
        request_id,
        call_stack
      };
      this.dataReceiver.saveRecord("callstacks", record);
    });
  }
}

/***/ }),

/***/ "./feature.js/loggingdb.js":
/*!*********************************!*\
  !*** ./feature.js/loggingdb.js ***!
  \*********************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "open": () => (/* binding */ open),
/* harmony export */   "close": () => (/* binding */ close),
/* harmony export */   "logInfo": () => (/* binding */ logInfo),
/* harmony export */   "logDebug": () => (/* binding */ logDebug),
/* harmony export */   "logWarn": () => (/* binding */ logWarn),
/* harmony export */   "logError": () => (/* binding */ logError),
/* harmony export */   "logCritical": () => (/* binding */ logCritical),
/* harmony export */   "dataReceiver": () => (/* binding */ dataReceiver),
/* harmony export */   "saveRecord": () => (/* binding */ saveRecord),
/* harmony export */   "saveContent": () => (/* binding */ saveContent),
/* harmony export */   "escapeString": () => (/* binding */ escapeString),
/* harmony export */   "boolToInt": () => (/* binding */ boolToInt)
/* harmony export */ });
/* harmony import */ var _socket_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ./socket.js */ "./feature.js/socket.js");


let crawlID = null;
let visitID = null;
let debugging = false;
let storageController = null;
let logAggregator = null;
let listeningSocket = null;


let listeningSocketCallback =  async (data) => {
    //This works even if data is an int
    let action = data["action"];
    let _visitID = data["visit_id"]
    switch (action) {
        case "Initialize":
            if (visitID) {
                logWarn("Set visit_id while another visit_id was set")
            }
            visitID = _visitID;
            data["browser_id"] = crawlID;
            storageController.send(JSON.stringify(["meta_information", data]));
            break;
        case "Finalize":
            if (!visitID) {
                logWarn("Send Finalize while no visit_id was set")
            }
            if (_visitID !== visitID ) {
                logError("Send Finalize but visit_id didn't match. " +
                `Current visit_id ${visit_id}, sent visit_id ${_visit_id}.`);
            }
            data["browser_id"] = crawlID;
            data["success"] = true;
            storageController.send(JSON.stringify(["meta_information", data]));
            visitID = null;
            break;
        default:
            // Just making sure that it's a valid number before logging
            _visitID = parseInt(data, 10);
            logDebug("Setting visit_id the legacy way");
            visitID = _visitID

    }

}
let open = async function(storageControllerAddress, logAddress, curr_crawlID) {
    if (storageControllerAddress == null && logAddress == null && curr_crawlID === 0) {
        console.log("Debugging, everything will output to console");
        debugging = true;
        return;
    }
    crawlID = curr_crawlID;

    console.log("Opening socket connections...");

    // Connect to MPLogger for extension info/debug/error logging
    if (logAddress != null) {
        logAggregator = new _socket_js__WEBPACK_IMPORTED_MODULE_0__.SendingSocket();
        let rv = await logAggregator.connect(logAddress[0], logAddress[1]);
        console.log("logSocket started?", rv)
    }

    // Connect to databases for saving data
    if (storageControllerAddress != null) {
        storageController = new _socket_js__WEBPACK_IMPORTED_MODULE_0__.SendingSocket();
        let rv = await storageController.connect(storageControllerAddress[0], storageControllerAddress[1]);
        console.log("StorageController started?",rv);
    }

    // Listen for incoming urls as visit ids
    listeningSocket = new _socket_js__WEBPACK_IMPORTED_MODULE_0__.ListeningSocket(listeningSocketCallback);
    console.log("Starting socket listening for incoming connections.");
    await listeningSocket.startListening().then(() => {
        browser.profileDirIO.writeFile("extension_port.txt", `${listeningSocket.port}`);
    });
};

let close = function() {
    if (storageController != null) {
        storageController.close();
    }
    if (logAggregator != null) {
        logAggregator.close();
    }
};

let makeLogJSON = function(lvl, msg) {
    var log_json = {
        'name': 'Extension-Logger',
        'level': lvl,
        'pathname': 'FirefoxExtension',
        'lineno': 1,
        'msg': escapeString(msg),
        'args': null,
        'exc_info': null,
        'func': null
    }
    return log_json;
}

let logInfo = function(msg) {
    // Always log to browser console
    console.log(msg);

    if (debugging) {
        return;
    }

    // Log level INFO == 20 (https://docs.python.org/2/library/logging.html#logging-levels)
    var log_json = makeLogJSON(20, msg);
    logAggregator.send(JSON.stringify(['EXT', JSON.stringify(log_json)]));
};

let logDebug = function(msg) {
    // Always log to browser console
    console.log(msg);

    if (debugging) {
        return;
    }

    // Log level DEBUG == 10 (https://docs.python.org/2/library/logging.html#logging-levels)
    var log_json = makeLogJSON(10, msg);
    logAggregator.send(JSON.stringify(['EXT', JSON.stringify(log_json)]));
};

let logWarn = function(msg) {
    // Always log to browser console
    console.warn(msg);

    if (debugging) {
        return;
    }

    // Log level WARN == 30 (https://docs.python.org/2/library/logging.html#logging-levels)
    var log_json = makeLogJSON(30, msg);
    logAggregator.send(JSON.stringify(['EXT', JSON.stringify(log_json)]));
};

let logError = function(msg) {
    // Always log to browser console
    console.error(msg);

    if (debugging) {
        return;
    }

    // Log level INFO == 40 (https://docs.python.org/2/library/logging.html#logging-levels)
    var log_json = makeLogJSON(40, msg);
    logAggregator.send(JSON.stringify(['EXT', JSON.stringify(log_json)]));
};

let logCritical = function(msg) {
    // Always log to browser console
    console.error(msg);

    if (debugging) {
        return;
    }

    // Log level CRITICAL == 50 (https://docs.python.org/2/library/logging.html#logging-levels)
    var log_json = makeLogJSON(50, msg);
    logAggregator.send(JSON.stringify(['EXT', JSON.stringify(log_json)]));
};

let dataReceiver = {
    saveRecord(a, b) {
        console.log(b);
    },
};

let saveRecord = function(instrument, record) {
    record["visit_id"] = visitID;

    if (!visitID && !debugging) {
        // Navigations to about:blank can be triggered by OpenWPM. We drop those.
        if(instrument === 'navigations' && record['url'] === 'about:blank') {
            logDebug('Extension-' + crawlID + ' : Dropping navigation to about:blank in intermediate period');
            return;
        }
        logWarn(`Extension-${crawlID} : visitID is null while attempting to insert into table ${instrument}\n` +
                    JSON.stringify(record));
        record["visit_id"] = -1;
        
    }

    // send to console if debugging
    if (debugging) {
      console.log("EXTENSION", instrument, record);
      return;
    }
    storageController.send(JSON.stringify([instrument, record]));
};

// Stub for now
let saveContent = async function(content, contentHash) {
  // Send page content to the data aggregator
  // deduplicated by contentHash in a levelDB database
  if (debugging) {
    console.log("LDB contentHash:",contentHash,"with length",content.length);
    return;
  }
  // Since the content might not be a valid utf8 string and it needs to be
  // json encoded later, it is encoded using base64 first.
  const b64 = Uint8ToBase64(content);
  storageController.send(JSON.stringify(['page_content', [b64, contentHash]]));
};

function encode_utf8(s) {
  return unescape(encodeURIComponent(s));
}

// Base64 encoding, found on:
// https://stackoverflow.com/questions/12710001/how-to-convert-uint8-array-to-base64-encoded-string/25644409#25644409
function Uint8ToBase64(u8Arr){
  var CHUNK_SIZE = 0x8000; //arbitrary number
  var index = 0;
  var length = u8Arr.length;
  var result = '';
  var slice;
  while (index < length) {
    slice = u8Arr.subarray(index, Math.min(index + CHUNK_SIZE, length));
    result += String.fromCharCode.apply(null, slice);
    index += CHUNK_SIZE;
  }
  return btoa(result);
}

let escapeString = function(string) {
    // Convert to string if necessary
    if(typeof string != "string")
        string = "" + string;

    return encode_utf8(string);
};

let boolToInt = function(bool) {
    return bool ? 1 : 0;
};


/***/ }),

/***/ "./feature.js/socket.js":
/*!******************************!*\
  !*** ./feature.js/socket.js ***!
  \******************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ListeningSocket": () => (/* binding */ ListeningSocket),
/* harmony export */   "SendingSocket": () => (/* binding */ SendingSocket)
/* harmony export */ });
let DataReceiver = {
  callbacks: new Map(),
  onDataReceived: (aSocketId, aData, aJSON) => {
    if (!DataReceiver.callbacks.has(aSocketId)) {
      return;
    }
    if (aJSON) {
      aData = JSON.parse(aData);
    }
    DataReceiver.callbacks.get(aSocketId)(aData);
  },
};

browser.sockets.onDataReceived.addListener(DataReceiver.onDataReceived);

let ListeningSockets = new Map();

class ListeningSocket {
  constructor(callback) {
    this.callback = callback
  }

  async startListening() {
    this.port = await browser.sockets.createServerSocket();
    DataReceiver.callbacks.set(this.port, this.callback);
    browser.sockets.startListening(this.port);
    console.log('Listening on port ' + this.port);
  }
}

class SendingSocket {
  constructor() {
  }

  async connect(host, port) {
    this.id = await browser.sockets.createSendingSocket();
    browser.sockets.connect(this.id, host, port);
    console.log(`Connected to ${host}:${port}`);
  }

  send(aData, aJSON=true) {
    try {
      browser.sockets.sendData(this.id, aData, !!aJSON);
      return true;
    } catch (err) {
      console.error(err,err.message);
      return false;
    }
  }

  close() {
    browser.sockets.close(this.id);
  }
}



/***/ }),

/***/ "../webext-instrumentation/build/module/background/cookie-instrument.js":
/*!******************************************************************************!*\
  !*** ../webext-instrumentation/build/module/background/cookie-instrument.js ***!
  \******************************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "transformCookieObjectToMatchOpenWPMSchema": () => (/* binding */ transformCookieObjectToMatchOpenWPMSchema),
/* harmony export */   "CookieInstrument": () => (/* binding */ CookieInstrument)
/* harmony export */ });
/* harmony import */ var _lib_extension_session_event_ordinal__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ../lib/extension-session-event-ordinal */ "../webext-instrumentation/build/module/lib/extension-session-event-ordinal.js");
/* harmony import */ var _lib_extension_session_uuid__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ../lib/extension-session-uuid */ "../webext-instrumentation/build/module/lib/extension-session-uuid.js");
/* harmony import */ var _lib_string_utils__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! ../lib/string-utils */ "../webext-instrumentation/build/module/lib/string-utils.js");



const transformCookieObjectToMatchOpenWPMSchema = (cookie) => {
    const javascriptCookie = {};
    // Expiry time (in seconds)
    // May return ~Max(int64). I believe this is a session
    // cookie which doesn't expire. Sessions cookies with
    // non-max expiry time expire after session or at expiry.
    const expiryTime = cookie.expirationDate; // returns seconds
    let expiryTimeString;
    const maxInt64 = 9223372036854776000;
    if (!cookie.expirationDate || expiryTime === maxInt64) {
        expiryTimeString = "9999-12-31T21:59:59.000Z";
    }
    else {
        const expiryTimeDate = new Date(expiryTime * 1000); // requires milliseconds
        expiryTimeString = expiryTimeDate.toISOString();
    }
    javascriptCookie.expiry = expiryTimeString;
    javascriptCookie.is_http_only = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.boolToInt)(cookie.httpOnly);
    javascriptCookie.is_host_only = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.boolToInt)(cookie.hostOnly);
    javascriptCookie.is_session = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.boolToInt)(cookie.session);
    javascriptCookie.host = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(cookie.domain);
    javascriptCookie.is_secure = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.boolToInt)(cookie.secure);
    javascriptCookie.name = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(cookie.name);
    javascriptCookie.path = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(cookie.path);
    javascriptCookie.value = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(cookie.value);
    javascriptCookie.same_site = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(cookie.sameSite);
    javascriptCookie.first_party_domain = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(cookie.firstPartyDomain);
    javascriptCookie.store_id = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(cookie.storeId);
    javascriptCookie.time_stamp = new Date().toISOString();
    return javascriptCookie;
};
class CookieInstrument {
    dataReceiver;
    onChangedListener;
    constructor(dataReceiver) {
        this.dataReceiver = dataReceiver;
    }
    run(crawlID) {
        // Instrument cookie changes
        this.onChangedListener = async (changeInfo) => {
            const eventType = changeInfo.removed ? "deleted" : "added-or-changed";
            const update = {
                record_type: eventType,
                change_cause: changeInfo.cause,
                browser_id: crawlID,
                extension_session_uuid: _lib_extension_session_uuid__WEBPACK_IMPORTED_MODULE_1__.extensionSessionUuid,
                event_ordinal: (0,_lib_extension_session_event_ordinal__WEBPACK_IMPORTED_MODULE_0__.incrementedEventOrdinal)(),
                ...transformCookieObjectToMatchOpenWPMSchema(changeInfo.cookie),
            };
            this.dataReceiver.saveRecord("javascript_cookies", update);
        };
        browser.cookies.onChanged.addListener(this.onChangedListener);
    }
    async saveAllCookies(crawlID) {
        const allCookies = await browser.cookies.getAll({});
        await Promise.all(allCookies.map((cookie) => {
            const update = {
                record_type: "manual-export",
                browser_id: crawlID,
                extension_session_uuid: _lib_extension_session_uuid__WEBPACK_IMPORTED_MODULE_1__.extensionSessionUuid,
                ...transformCookieObjectToMatchOpenWPMSchema(cookie),
            };
            return this.dataReceiver.saveRecord("javascript_cookies", update);
        }));
    }
    cleanup() {
        if (this.onChangedListener) {
            browser.cookies.onChanged.removeListener(this.onChangedListener);
        }
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29va2llLWluc3RydW1lbnQuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi9zcmMvYmFja2dyb3VuZC9jb29raWUtaW5zdHJ1bWVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQSxPQUFPLEVBQUUsdUJBQXVCLEVBQUUsTUFBTSx3Q0FBd0MsQ0FBQztBQUNqRixPQUFPLEVBQUUsb0JBQW9CLEVBQUUsTUFBTSwrQkFBK0IsQ0FBQztBQUNyRSxPQUFPLEVBQUUsU0FBUyxFQUFFLFlBQVksRUFBRSxNQUFNLHFCQUFxQixDQUFDO0FBSzlELE1BQU0sQ0FBQyxNQUFNLHlDQUF5QyxHQUFHLENBQUMsTUFBYyxFQUFFLEVBQUU7SUFDMUUsTUFBTSxnQkFBZ0IsR0FBRyxFQUFzQixDQUFDO0lBRWhELDJCQUEyQjtJQUMzQixzREFBc0Q7SUFDdEQscURBQXFEO0lBQ3JELHlEQUF5RDtJQUN6RCxNQUFNLFVBQVUsR0FBRyxNQUFNLENBQUMsY0FBYyxDQUFDLENBQUMsa0JBQWtCO0lBQzVELElBQUksZ0JBQWdCLENBQUM7SUFDckIsTUFBTSxRQUFRLEdBQUcsbUJBQW1CLENBQUM7SUFDckMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxjQUFjLElBQUksVUFBVSxLQUFLLFFBQVEsRUFBRTtRQUNyRCxnQkFBZ0IsR0FBRywwQkFBMEIsQ0FBQztLQUMvQztTQUFNO1FBQ0wsTUFBTSxjQUFjLEdBQUcsSUFBSSxJQUFJLENBQUMsVUFBVSxHQUFHLElBQUksQ0FBQyxDQUFDLENBQUMsd0JBQXdCO1FBQzVFLGdCQUFnQixHQUFHLGNBQWMsQ0FBQyxXQUFXLEVBQUUsQ0FBQztLQUNqRDtJQUNELGdCQUFnQixDQUFDLE1BQU0sR0FBRyxnQkFBZ0IsQ0FBQztJQUMzQyxnQkFBZ0IsQ0FBQyxZQUFZLEdBQUcsU0FBUyxDQUFDLE1BQU0sQ0FBQyxRQUFRLENBQUMsQ0FBQztJQUMzRCxnQkFBZ0IsQ0FBQyxZQUFZLEdBQUcsU0FBUyxDQUFDLE1BQU0sQ0FBQyxR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

/***/ }),

/***/ "../webext-instrumentation/build/module/background/dns-instrument.js":
/*!***************************************************************************!*\
  !*** ../webext-instrumentation/build/module/background/dns-instrument.js ***!
  \***************************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "DnsInstrument": () => (/* binding */ DnsInstrument)
/* harmony export */ });
/* harmony import */ var _lib_pending_response__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ../lib/pending-response */ "../webext-instrumentation/build/module/lib/pending-response.js");
/* harmony import */ var _http_instrument__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ./http-instrument */ "../webext-instrumentation/build/module/background/http-instrument.js");


class DnsInstrument {
    dataReceiver;
    onCompleteListener;
    pendingResponses = {};
    constructor(dataReceiver) {
        this.dataReceiver = dataReceiver;
    }
    run(crawlID) {
        const filter = { urls: ["<all_urls>"], types: _http_instrument__WEBPACK_IMPORTED_MODULE_1__.allTypes };
        const requestStemsFromExtension = (details) => {
            return (details.originUrl &&
                details.originUrl.indexOf("moz-extension://") > -1 &&
                details.originUrl.includes("fakeRequest"));
        };
        /*
         * Attach handlers to event listeners
         */
        this.onCompleteListener = (details) => {
            // Ignore requests made by extensions
            if (requestStemsFromExtension(details)) {
                return;
            }
            const pendingResponse = this.getPendingResponse(details.requestId);
            pendingResponse.resolveOnCompletedEventDetails(details);
            this.onCompleteDnsHandler(details, crawlID);
        };
        browser.webRequest.onCompleted.addListener(this.onCompleteListener, filter);
    }
    cleanup() {
        if (this.onCompleteListener) {
            browser.webRequest.onCompleted.removeListener(this.onCompleteListener);
        }
    }
    getPendingResponse(requestId) {
        if (!this.pendingResponses[requestId]) {
            this.pendingResponses[requestId] = new _lib_pending_response__WEBPACK_IMPORTED_MODULE_0__.PendingResponse();
        }
        return this.pendingResponses[requestId];
    }
    handleResolvedDnsData(dnsRecordObj, dataReceiver) {
        // Curring the data returned by API call.
        return function (record) {
            // Get data from API call
            dnsRecordObj.addresses = record.addresses.toString();
            dnsRecordObj.canonical_name = record.canonicalName;
            dnsRecordObj.is_TRR = record.isTRR;
            // Send data to main OpenWPM data aggregator.
            dataReceiver.saveRecord("dns_responses", dnsRecordObj);
        };
    }
    async onCompleteDnsHandler(details, crawlID) {
        // Create and populate DnsResolve object
        const dnsRecord = {};
        dnsRecord.browser_id = crawlID;
        dnsRecord.request_id = Number(details.requestId);
        dnsRecord.used_address = details.ip;
        const currentTime = new Date(details.timeStamp);
        dnsRecord.time_stamp = currentTime.toISOString();
        // Query DNS API
        const url = new URL(details.url);
        dnsRecord.hostname = url.hostname;
        const dnsResolve = browser.dns.resolve(dnsRecord.hostname, [
            "canonical_name",
        ]);
        dnsResolve.then(this.handleResolvedDnsData(dnsRecord, this.dataReceiver));
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZG5zLWluc3RydW1lbnQuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi9zcmMvYmFja2dyb3VuZC9kbnMtaW5zdHJ1bWVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQSxPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0seUJBQXlCLENBQUM7QUFHMUQsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLG1CQUFtQixDQUFDO0FBRzdDLE1BQU0sT0FBTyxhQUFhO0lBQ1AsWUFBWSxDQUFDO0lBQ3RCLGtCQUFrQixDQUFDO0lBQ25CLGdCQUFnQixHQUVwQixFQUFFLENBQUM7SUFFUCxZQUFZLFlBQVk7UUFDdEIsSUFBSSxDQUFDLFlBQVksR0FBRyxZQUFZLENBQUM7SUFDbkMsQ0FBQztJQUVNLEdBQUcsQ0FBQyxPQUFPO1FBQ2hCLE1BQU0sTUFBTSxHQUFrQixFQUFFLElBQUksRUFBRSxDQUFDLFlBQVksQ0FBQyxFQUFFLEtBQUssRUFBRSxRQUFRLEVBQUUsQ0FBQztRQUV4RSxNQUFNLHlCQUF5QixHQUFHLENBQUMsT0FBTyxFQUFFLEVBQUU7WUFDNUMsT0FBTyxDQUNMLE9BQU8sQ0FBQyxTQUFTO2dCQUNqQixPQUFPLENBQUMsU0FBUyxDQUFDLE9BQU8sQ0FBQyxrQkFBa0IsQ0FBQyxHQUFHLENBQUMsQ0FBQztnQkFDbEQsT0FBTyxDQUFDLFNBQVMsQ0FBQyxRQUFRLENBQUMsYUFBYSxDQUFDLENBQzFDLENBQUM7UUFDSixDQUFDLENBQUM7UUFFRjs7V0FFRztRQUNILElBQUksQ0FBQyxrQkFBa0IsR0FBRyxDQUFDLE9BQTBDLEVBQUUsRUFBRTtZQUN2RSxxQ0FBcUM7WUFDckMsSUFBSSx5QkFBeUIsQ0FBQyxPQUFPLENBQUMsRUFBRTtnQkFDdEMsT0FBTzthQUNSO1lBQ0QsTUFBTSxlQUFlLEdBQUcsSUFBSSxDQUFDLGtCQUFrQixDQUFDLE9BQU8sQ0FBQyxTQUFTLENBQUMsQ0FBQztZQUNuRSxlQUFlLENBQUMsOEJBQThCLENBQUMs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

/***/ }),

/***/ "../webext-instrumentation/build/module/background/http-instrument.js":
/*!****************************************************************************!*\
  !*** ../webext-instrumentation/build/module/background/http-instrument.js ***!
  \****************************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "allTypes": () => (/* binding */ allTypes),
/* harmony export */   "HttpInstrument": () => (/* binding */ HttpInstrument)
/* harmony export */ });
/* harmony import */ var _lib_extension_session_event_ordinal__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ../lib/extension-session-event-ordinal */ "../webext-instrumentation/build/module/lib/extension-session-event-ordinal.js");
/* harmony import */ var _lib_extension_session_uuid__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ../lib/extension-session-uuid */ "../webext-instrumentation/build/module/lib/extension-session-uuid.js");
/* harmony import */ var _lib_http_post_parser__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! ../lib/http-post-parser */ "../webext-instrumentation/build/module/lib/http-post-parser.js");
/* harmony import */ var _lib_pending_request__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(/*! ../lib/pending-request */ "../webext-instrumentation/build/module/lib/pending-request.js");
/* harmony import */ var _lib_pending_response__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(/*! ../lib/pending-response */ "../webext-instrumentation/build/module/lib/pending-response.js");
/* harmony import */ var _lib_string_utils__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(/*! ../lib/string-utils */ "../webext-instrumentation/build/module/lib/string-utils.js");






/**
 * Note: Different parts of the desired information arrives in different events as per below:
 * request = headers in onBeforeSendHeaders + body in onBeforeRequest
 * response = headers in onCompleted + body via a onBeforeRequest filter
 * redirect = original request headers+body, followed by a onBeforeRedirect and then a new set of request headers+body and response headers+body
 * Docs: https://developer.mozilla.org/en-US/docs/User:wbamberg/webRequest.RequestDetails
 */
const allTypes = [
    "beacon",
    "csp_report",
    "font",
    "image",
    "imageset",
    "main_frame",
    "media",
    "object",
    "object_subrequest",
    "ping",
    "script",
    "speculative",
    "stylesheet",
    "sub_frame",
    "web_manifest",
    "websocket",
    "xml_dtd",
    "xmlhttprequest",
    "xslt",
    "other",
];

class HttpInstrument {
    dataReceiver;
    pendingRequests = {};
    pendingResponses = {};
    onBeforeRequestListener;
    onBeforeSendHeadersListener;
    onBeforeRedirectListener;
    onCompletedListener;
    constructor(dataReceiver) {
        this.dataReceiver = dataReceiver;
    }
    run(crawlID, saveContentOption) {
        const filter = { urls: ["<all_urls>"], types: allTypes };
        const requestStemsFromExtension = (details) => {
            return (details.originUrl && details.originUrl.indexOf("moz-extension://") > -1);
        };
        /*
         * Attach handlers to event listeners
         */
        this.onBeforeRequestListener = (details) => {
            const blockingResponseThatDoesNothing = {};
            // Ignore requests made by extensions
            if (requestStemsFromExtension(details)) {
                return blockingResponseThatDoesNothing;
            }
            const pendingRequest = this.getPendingRequest(details.requestId);
            pendingRequest.resolveOnBeforeRequestEventDetails(details);
            const pendingResponse = this.getPendingResponse(details.requestId);
            pendingResponse.resolveOnBeforeRequestEventDetails(details);
            if (this.shouldSaveContent(saveContentOption, details.type)) {
                pendingResponse.addResponseResponseBodyListener(details);
            }
            return blockingResponseThatDoesNothing;
        };
        browser.webRequest.onBeforeRequest.addListener(this.onBeforeRequestListener, filter, this.isContentSavingEnabled(saveContentOption)
            ? ["requestBody", "blocking"]
            : ["requestBody"]);
        this.onBeforeSendHeadersListener = (details) => {
            // Ignore requests made by extensions
            if (requestStemsFromExtension(details)) {
                return;
            }
            const pendingRequest = this.getPendingRequest(details.requestId);
            pendingRequest.resolveOnBeforeSendHeadersEventDetails(details);
            this.onBeforeSendHeadersHandler(details, crawlID, (0,_lib_extension_session_event_ordinal__WEBPACK_IMPORTED_MODULE_0__.incrementedEventOrdinal)());
        };
        browser.webRequest.onBeforeSendHeaders.addListener(this.onBeforeSendHeadersListener, filter, ["requestHeaders"]);
        this.onBeforeRedirectListener = (details) => {
            // Ignore requests made by extensions
            if (requestStemsFromExtension(details)) {
                return;
            }
            this.onBeforeRedirectHandler(details, crawlID, (0,_lib_extension_session_event_ordinal__WEBPACK_IMPORTED_MODULE_0__.incrementedEventOrdinal)());
        };
        browser.webRequest.onBeforeRedirect.addListener(this.onBeforeRedirectListener, filter, ["responseHeaders"]);
        this.onCompletedListener = (details) => {
            // Ignore requests made by extensions
            if (requestStemsFromExtension(details)) {
                return;
            }
            const pendingResponse = this.getPendingResponse(details.requestId);
            pendingResponse.resolveOnCompletedEventDetails(details);
            this.onCompletedHandler(details, crawlID, (0,_lib_extension_session_event_ordinal__WEBPACK_IMPORTED_MODULE_0__.incrementedEventOrdinal)(), saveContentOption);
        };
        browser.webRequest.onCompleted.addListener(this.onCompletedListener, filter, ["responseHeaders"]);
    }
    cleanup() {
        if (this.onBeforeRequestListener) {
            browser.webRequest.onBeforeRequest.removeListener(this.onBeforeRequestListener);
        }
        if (this.onBeforeSendHeadersListener) {
            browser.webRequest.onBeforeSendHeaders.removeListener(this.onBeforeSendHeadersListener);
        }
        if (this.onBeforeRedirectListener) {
            browser.webRequest.onBeforeRedirect.removeListener(this.onBeforeRedirectListener);
        }
        if (this.onCompletedListener) {
            browser.webRequest.onCompleted.removeListener(this.onCompletedListener);
        }
    }
    isContentSavingEnabled(saveContentOption) {
        if (saveContentOption === true) {
            return true;
        }
        if (saveContentOption === false) {
            return false;
        }
        return this.saveContentResourceTypes(saveContentOption).length > 0;
    }
    saveContentResourceTypes(saveContentOption) {
        return saveContentOption.split(",");
    }
    /**
     * We rely on the resource type to filter responses
     * See: https://developer.mozilla.org/en-US/docs/Mozilla/Add-ons/WebExtensions/API/webRequest/ResourceType
     *
     * @param saveContentOption
     * @param resourceType
     */
    shouldSaveContent(saveContentOption, resourceType) {
        if (saveContentOption === true) {
            return true;
        }
        if (saveContentOption === false) {
            return false;
        }
        return this.saveContentResourceTypes(saveContentOption).includes(resourceType);
    }
    getPendingRequest(requestId) {
        if (!this.pendingRequests[requestId]) {
            this.pendingRequests[requestId] = new _lib_pending_request__WEBPACK_IMPORTED_MODULE_3__.PendingRequest();
        }
        return this.pendingRequests[requestId];
    }
    getPendingResponse(requestId) {
        if (!this.pendingResponses[requestId]) {
            this.pendingResponses[requestId] = new _lib_pending_response__WEBPACK_IMPORTED_MODULE_4__.PendingResponse();
        }
        return this.pendingResponses[requestId];
    }
    /*
     * HTTP Request Handler and Helper Functions
     */
    async onBeforeSendHeadersHandler(details, crawlID, eventOrdinal) {
        const tab = details.tabId > -1
            ? await browser.tabs.get(details.tabId)
            : { windowId: undefined, incognito: undefined, url: undefined };
        const update = {};
        update.incognito = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.boolToInt)(tab.incognito);
        update.browser_id = crawlID;
        update.extension_session_uuid = _lib_extension_session_uuid__WEBPACK_IMPORTED_MODULE_1__.extensionSessionUuid;
        update.event_ordinal = eventOrdinal;
        update.window_id = tab.windowId;
        update.tab_id = details.tabId;
        update.frame_id = details.frameId;
        // requestId is a unique identifier that can be used to link requests and responses
        update.request_id = Number(details.requestId);
        const url = details.url;
        update.url = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeUrl)(url);
        const requestMethod = details.method;
        update.method = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(requestMethod);
        const current_time = new Date(details.timeStamp);
        update.time_stamp = current_time.toISOString();
        let encodingType = "";
        let referrer = "";
        const headers = [];
        let isOcsp = false;
        if (details.requestHeaders) {
            details.requestHeaders.map((requestHeader) => {
                const { name, value } = requestHeader;
                const header_pair = [];
                header_pair.push((0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(name));
                header_pair.push((0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(value));
                headers.push(header_pair);
                if (name === "Content-Type") {
                    encodingType = value;
                    if (encodingType.indexOf("application/ocsp-request") !== -1) {
                        isOcsp = true;
                    }
                }
                if (name === "Referer") {
                    referrer = value;
                }
            });
        }
        update.referrer = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(referrer);
        if (requestMethod === "POST" && !isOcsp /* don't process OCSP requests */) {
            const pendingRequest = this.getPendingRequest(details.requestId);
            const resolved = await pendingRequest.resolvedWithinTimeout(1000);
            if (!resolved) {
                this.dataReceiver.logError("Pending request timed out waiting for data from both onBeforeRequest and onBeforeSendHeaders events");
            }
            else {
                const onBeforeRequestEventDetails = await pendingRequest.onBeforeRequestEventDetails;
                const requestBody = onBeforeRequestEventDetails.requestBody;
                if (requestBody) {
                    const postParser = new _lib_http_post_parser__WEBPACK_IMPORTED_MODULE_2__.HttpPostParser(onBeforeRequestEventDetails, this.dataReceiver);
                    const postObj = postParser.parsePostRequest();
                    // Add (POST) request headers from upload stream
                    if ("post_headers" in postObj) {
                        // Only store POST headers that we know and need. We may misinterpret POST data as headers
                        // as detection is based on "key:value" format (non-header POST data can be in this format as well)
                        const contentHeaders = [
                            "Content-Type",
                            "Content-Disposition",
                            "Content-Length",
                        ];
                        for (const name in postObj.post_headers) {
                            if (contentHeaders.includes(name)) {
                                const header_pair = [];
                                header_pair.push((0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(name));
                                header_pair.push((0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(postObj.post_headers[name]));
                                headers.push(header_pair);
                            }
                        }
                    }
                    // we store POST body in JSON format, except when it's a string without a (key-value) structure
                    if ("post_body" in postObj) {
                        update.post_body = postObj.post_body;
                    }
                    if ("post_body_raw" in postObj) {
                        update.post_body_raw = postObj.post_body_raw;
                    }
                }
            }
        }
        update.headers = JSON.stringify(headers);
        // Check if xhr
        const isXHR = details.type === "xmlhttprequest";
        update.is_XHR = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.boolToInt)(isXHR);
        // Grab the triggering and loading Principals
        let triggeringOrigin;
        let loadingOrigin;
        if (details.originUrl) {
            const parsedOriginUrl = new URL(details.originUrl);
            triggeringOrigin = parsedOriginUrl.origin;
        }
        if (details.documentUrl) {
            const parsedDocumentUrl = new URL(details.documentUrl);
            loadingOrigin = parsedDocumentUrl.origin;
        }
        update.triggering_origin = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(triggeringOrigin);
        update.loading_origin = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(loadingOrigin);
        // loadingDocument's href
        // The loadingDocument is the document the element resides, regardless of
        // how the load was triggered.
        const loadingHref = details.documentUrl;
        update.loading_href = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(loadingHref);
        // resourceType of the requesting node. This is set by the type of
        // node making the request (i.e. an <img src=...> node will set to type "image").
        // Documentation:
        // https://developer.mozilla.org/en-US/docs/Mozilla/Add-ons/WebExtensions/API/webRequest/ResourceType
        update.resource_type = details.type;
        /*
        // TODO: Refactor to corresponding webext logic or discard
        const ThirdPartyUtil = Cc["@mozilla.org/thirdpartyutil;1"].getService(
                               Ci.mozIThirdPartyUtil);
        // Do third-party checks
        // These specific checks are done because it's what's used in Tracking Protection
        // See: http://searchfox.org/mozilla-central/source/netwerk/base/nsChannelClassifier.cpp#107
        try {
          const isThirdPartyChannel = ThirdPartyUtil.isThirdPartyChannel(details);
          const topWindow = ThirdPartyUtil.getTopWindowForChannel(details);
          const topURI = ThirdPartyUtil.getURIFromWindow(topWindow);
          if (topURI) {
            const topUrl = topURI.spec;
            const channelURI = details.URI;
            const isThirdPartyToTopWindow = ThirdPartyUtil.isThirdPartyURI(
              channelURI,
              topURI,
            );
            update.is_third_party_to_top_window = isThirdPartyToTopWindow;
            update.is_third_party_channel = isThirdPartyChannel;
          }
        } catch (anError) {
          // Exceptions expected for channels triggered or loading in a
          // NullPrincipal or SystemPrincipal. They are also expected for favicon
          // loads, which we attempt to filter. Depending on the naming, some favicons
          // may continue to lead to error logs.
          if (
            update.triggering_origin !== "[System Principal]" &&
            update.triggering_origin !== undefined &&
            update.loading_origin !== "[System Principal]" &&
            update.loading_origin !== undefined &&
            !update.url.endsWith("ico")
          ) {
            this.dataReceiver.logError(
              "Error while retrieving additional channel information for URL: " +
              "\n" +
              update.url +
              "\n Error text:" +
              JSON.stringify(anError),
            );
          }
        }
        */
        update.top_level_url = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeUrl)(this.getDocumentUrlForRequest(details));
        update.parent_frame_id = details.parentFrameId;
        update.frame_ancestors = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(JSON.stringify(details.frameAncestors));
        this.dataReceiver.saveRecord("http_requests", update);
    }
    /**
     * Code taken and adapted from
     * https://github.com/EFForg/privacybadger/pull/2198/files
     *
     * Gets the URL for a given request's top-level document.
     *
     * The request's document may be different from the current top-level document
     * loaded in tab as requests can come out of order:
     *
     * @param {WebRequestOnBeforeSendHeadersEventDetails} details
     *
     * @return {?String} the URL for the request's top-level document
     */
    getDocumentUrlForRequest(details) {
        let url = "";
        if (details.type === "main_frame") {
            // Url of the top-level document itself.
            url = details.url;
        }
        else if (details.hasOwnProperty("frameAncestors")) {
            // In case of nested frames, retrieve url from top-most ancestor.
            // If frameAncestors == [], request comes from the top-level-document.
            url = details.frameAncestors.length
                ? details.frameAncestors[details.frameAncestors.length - 1].url
                : details.documentUrl;
        }
        else {
            // type != 'main_frame' and frameAncestors == undefined
            // For example service workers: https://bugzilla.mozilla.org/show_bug.cgi?id=1470537#c13
            url = details.documentUrl;
        }
        return url;
    }
    async onBeforeRedirectHandler(details, crawlID, eventOrdinal) {
        /*
        console.log(
          "onBeforeRedirectHandler (previously httpRequestHandler)",
          details,
          crawlID,
        );
        */
        // Save HTTP redirect events
        // Events are saved to the `http_redirects` table
        /*
        // TODO: Refactor to corresponding webext logic or discard
        // Events are saved to the `http_redirects` table, and map the old
        // request/response channel id to the new request/response channel id.
        // Implementation based on: https://stackoverflow.com/a/11240627
        const oldNotifications = details.notificationCallbacks;
        let oldEventSink = null;
        details.notificationCallbacks = {
          QueryInterface: XPCOMUtils.generateQI([
            Ci.nsIInterfaceRequestor,
            Ci.nsIChannelEventSink,
          ]),
    
          getInterface(iid) {
            // We are only interested in nsIChannelEventSink,
            // return the old callbacks for any other interface requests.
            if (iid.equals(Ci.nsIChannelEventSink)) {
              try {
                oldEventSink = oldNotifications.QueryInterface(iid);
              } catch (anError) {
                this.dataReceiver.logError(
                  "Error during call to custom notificationCallbacks::getInterface." +
                    JSON.stringify(anError),
                );
              }
              return this;
            }
    
            if (oldNotifications) {
              return oldNotifications.getInterface(iid);
            } else {
              throw Cr.NS_ERROR_NO_INTERFACE;
            }
          },
    
          asyncOnChannelRedirect(oldChannel, newChannel, flags, callback) {
    
            newChannel.QueryInterface(Ci.nsIHttpChannel);
    
            const httpRedirect: HttpRedirect = {
              browser_id: crawlID,
              old_request_id: oldChannel.channelId,
              new_request_id: newChannel.channelId,
              time_stamp: new Date().toISOString(),
            };
            this.dataReceiver.saveRecord("http_redirects", httpRedirect);
    
            if (oldEventSink) {
              oldEventSink.asyncOnChannelRedirect(
                oldChannel,
                newChannel,
                flags,
                callback,
              );
            } else {
              callback.onRedirectVerifyCallback(Cr.NS_OK);
            }
          },
        };
        */
        const responseStatus = details.statusCode;
        const responseStatusText = details.statusLine;
        const tab = details.tabId > -1
            ? await browser.tabs.get(details.tabId)
            : { windowId: undefined, incognito: undefined };
        const httpRedirect = {
            incognito: (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.boolToInt)(tab.incognito),
            browser_id: crawlID,
            old_request_url: (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeUrl)(details.url),
            old_request_id: details.requestId,
            new_request_url: (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeUrl)(details.redirectUrl),
            new_request_id: null,
            extension_session_uuid: _lib_extension_session_uuid__WEBPACK_IMPORTED_MODULE_1__.extensionSessionUuid,
            event_ordinal: eventOrdinal,
            window_id: tab.windowId,
            tab_id: details.tabId,
            frame_id: details.frameId,
            response_status: responseStatus,
            response_status_text: (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(responseStatusText),
            headers: this.jsonifyHeaders(details.responseHeaders).headers,
            time_stamp: new Date(details.timeStamp).toISOString(),
        };
        this.dataReceiver.saveRecord("http_redirects", httpRedirect);
    }
    /*
     * HTTP Response Handlers and Helper Functions
     */
    async logWithResponseBody(details, update) {
        const pendingResponse = this.getPendingResponse(details.requestId);
        try {
            const responseBodyListener = pendingResponse.responseBodyListener;
            const respBody = await responseBodyListener.getResponseBody();
            const contentHash = await responseBodyListener.getContentHash();
            this.dataReceiver.saveContent(respBody, (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(contentHash));
            update.content_hash = contentHash;
            this.dataReceiver.saveRecord("http_responses", update);
        }
        catch (err) {
            /*
            // TODO: Refactor to corresponding webext logic or discard
            dataReceiver.logError(
              "Unable to retrieve response body." + JSON.stringify(aReason),
            );
            update.content_hash = "<error>";
            dataReceiver.saveRecord("http_responses", update);
            */
            this.dataReceiver.logError("Unable to retrieve response body." +
                "Likely caused by a programming error. Error Message:" +
                err.name +
                err.message +
                "\n" +
                err.stack);
            update.content_hash = "<error>";
            this.dataReceiver.saveRecord("http_responses", update);
        }
    }
    // Instrument HTTP responses
    async onCompletedHandler(details, crawlID, eventOrdinal, saveContent) {
        /*
        console.log(
          "onCompletedHandler (previously httpRequestHandler)",
          details,
          crawlID,
          saveContent,
        );
        */
        const tab = details.tabId > -1
            ? await browser.tabs.get(details.tabId)
            : { windowId: undefined, incognito: undefined };
        const update = {};
        update.incognito = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.boolToInt)(tab.incognito);
        update.browser_id = crawlID;
        update.extension_session_uuid = _lib_extension_session_uuid__WEBPACK_IMPORTED_MODULE_1__.extensionSessionUuid;
        update.event_ordinal = eventOrdinal;
        update.window_id = tab.windowId;
        update.tab_id = details.tabId;
        update.frame_id = details.frameId;
        // requestId is a unique identifier that can be used to link requests and responses
        update.request_id = Number(details.requestId);
        const isCached = details.fromCache;
        update.is_cached = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.boolToInt)(isCached);
        const url = details.url;
        update.url = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeUrl)(url);
        const requestMethod = details.method;
        update.method = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(requestMethod);
        // TODO: Refactor to corresponding webext logic or discard
        // (request headers are not available in http response event listener object,
        // but the referrer property of the corresponding request could be queried)
        //
        // let referrer = "";
        // if (details.referrer) {
        //   referrer = details.referrer.spec;
        // }
        // update.referrer = escapeString(referrer);
        const responseStatus = details.statusCode;
        update.response_status = responseStatus;
        const responseStatusText = details.statusLine;
        update.response_status_text = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(responseStatusText);
        const current_time = new Date(details.timeStamp);
        update.time_stamp = current_time.toISOString();
        const parsedHeaders = this.jsonifyHeaders(details.responseHeaders);
        update.headers = parsedHeaders.headers;
        update.location = parsedHeaders.location;
        if (this.shouldSaveContent(saveContent, details.type)) {
            this.logWithResponseBody(details, update);
        }
        else {
            this.dataReceiver.saveRecord("http_responses", update);
        }
    }
    jsonifyHeaders(headers) {
        const resultHeaders = [];
        let location = "";
        if (headers) {
            headers.map((responseHeader) => {
                const { name, value } = responseHeader;
                const header_pair = [];
                header_pair.push((0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(name));
                header_pair.push((0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(value));
                resultHeaders.push(header_pair);
                if (name.toLowerCase() === "location") {
                    location = value;
                }
            });
        }
        return {
            headers: JSON.stringify(resultHeaders),
            location: (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_5__.escapeString)(location),
        };
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaHR0cC1pbnN0cnVtZW50LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vc3JjL2JhY2tncm91bmQvaHR0cC1pbnN0cnVtZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBLE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxNQUFNLHdDQUF3QyxDQUFDO0FBQ2pGLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxNQUFNLCtCQUErQixDQUFDO0FBQ3JFLE9BQU8sRUFBRSxjQUFjLEVBQXFCLE1BQU0seUJBQXlCLENBQUM7QUFDNUUsT0FBTyxFQUFFLGNBQWMsRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBQ3hELE9BQU8sRUFBRSxlQUFlLEVBQUUsTUFBTSx5QkFBeUIsQ0FBQztBQUMxRCxPQUFPLEVBQUUsU0FBUyxFQUFFLFlBQVksRUFBRSxTQUFTLEVBQUUsTUFBTSxxQkFBcUIsQ0FBQztBQWV6RTs7Ozs7O0dBTUc7QUFFSCxNQUFNLFFBQVEsR0FBbUI7SUFDL0IsUUFBUTtJQUNSLFlBQVk7SUFDWixNQUFNO0lBQ04sT0FBTztJQUNQLFVBQVU7SUFDVixZQUFZO0lBQ1osT0FBTztJQUNQLFFBQVE7SUFDUixtQkFBbUI7SUFDbkIsTUFBTTtJQUNOLFFBQVE7SUFDUixhQUFhO0lBQ2IsWUFBWTtJQUNaLFdBQVc7SUFDWCxjQUFjO0lBQ2QsV0FBVztJQUNYLFNBQVM7SUFDVCxnQkFBZ0I7SUFDaEIsTUFBTTtJQUNOLE9BQU87Q0FDUixDQUFDO0FBRUYsT0FBTyxFQUFFLFFBQVEsRUFBRSxDQUFDO0FBRXBCLE1BQU0sT0FBTyxjQUFjO0lBQ1IsWUFBWSxDQUFDO0lBQ3RCLGVBQWUsR0FFbkIsRUFBRSxDQUFDO0lBQ0MsZ0JBQWdCLEdBRXBCLEVBQUUsQ0FBQztJQUNDLHVCQUF1QixDQUFDO0lBQ3hCLDJCQUEyQixDQUFDO0lBQzVCLHdCQUF3QixDQUFDO0lBQ3pCLG1CQUFtQixDQUFDO0lBRTVCLFlBQVksWUFBWTtRQUN0QixJQUFJLENBQUMsWUFB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

/***/ }),

/***/ "../webext-instrumentation/build/module/background/javascript-instrument.js":
/*!**********************************************************************************!*\
  !*** ../webext-instrumentation/build/module/background/javascript-instrument.js ***!
  \**********************************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "JavascriptInstrument": () => (/* binding */ JavascriptInstrument)
/* harmony export */ });
/* harmony import */ var _lib_extension_session_event_ordinal__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ../lib/extension-session-event-ordinal */ "../webext-instrumentation/build/module/lib/extension-session-event-ordinal.js");
/* harmony import */ var _lib_extension_session_uuid__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ../lib/extension-session-uuid */ "../webext-instrumentation/build/module/lib/extension-session-uuid.js");
/* harmony import */ var _lib_string_utils__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! ../lib/string-utils */ "../webext-instrumentation/build/module/lib/string-utils.js");



class JavascriptInstrument {
    /**
     * Converts received call and values data from the JS Instrumentation
     * into the format that the schema expects.
     *
     * @param data
     * @param sender
     */
    static processCallsAndValues(data, sender) {
        const update = {};
        update.extension_session_uuid = _lib_extension_session_uuid__WEBPACK_IMPORTED_MODULE_1__.extensionSessionUuid;
        update.event_ordinal = (0,_lib_extension_session_event_ordinal__WEBPACK_IMPORTED_MODULE_0__.incrementedEventOrdinal)();
        update.page_scoped_event_ordinal = data.ordinal;
        update.window_id = sender.tab.windowId;
        update.tab_id = sender.tab.id;
        update.frame_id = sender.frameId;
        update.script_url = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeUrl)(data.scriptUrl);
        update.script_line = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(data.scriptLine);
        update.script_col = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(data.scriptCol);
        update.func_name = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(data.funcName);
        update.script_loc_eval = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(data.scriptLocEval);
        update.call_stack = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(data.callStack);
        update.symbol = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(data.symbol);
        update.operation = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(data.operation);
        update.value = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(data.value);
        update.attributes = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(data.attributes);
        update.time_stamp = data.timeStamp;
        update.incognito = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.boolToInt)(sender.tab.incognito);
        // document_url is the current frame's document href
        // top_level_url is the top-level frame's document href
        update.document_url = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeUrl)(sender.url);
        update.top_level_url = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeUrl)(sender.tab.url);
        if (data.operation === "call" && data.args.length > 0) {
            update.arguments = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_2__.escapeString)(JSON.stringify(data.args));
        }
        return update;
    }
    dataReceiver;
    onMessageListener;
    configured = false;
    pendingRecords = [];
    crawlID;
    constructor(dataReceiver) {
        this.dataReceiver = dataReceiver;
    }
    /**
     * Start listening for messages from page/content/background scripts injected to instrument JavaScript APIs
     */
    listen() {
        this.onMessageListener = (message, sender) => {
            if (message.namespace &&
                message.namespace === "javascript-instrumentation") {
                this.handleJsInstrumentationMessage(message, sender);
            }
        };
        browser.runtime.onMessage.addListener(this.onMessageListener);
    }
    /**
     * Either sends the log data to the dataReceiver or store it in memory
     * as a pending record if the JS instrumentation is not yet configured
     *
     * @param message
     * @param sender
     */
    handleJsInstrumentationMessage(message, sender) {
        switch (message.type) {
            case "logCall":
            case "logValue":
                const update = JavascriptInstrument.processCallsAndValues(message.data, sender);
                if (this.configured) {
                    update.browser_id = this.crawlID;
                    this.dataReceiver.saveRecord("javascript", update);
                }
                else {
                    this.pendingRecords.push(update);
                }
                break;
        }
    }
    /**
     * Starts listening if haven't done so already, sets the crawl ID,
     * marks the JS instrumentation as configured and sends any pending
     * records that have been received up until this point.
     *
     * @param crawlID
     */
    run(crawlID) {
        if (!this.onMessageListener) {
            this.listen();
        }
        this.crawlID = crawlID;
        this.configured = true;
        this.pendingRecords.map((update) => {
            update.browser_id = this.crawlID;
            this.dataReceiver.saveRecord("javascript", update);
        });
    }
    async registerContentScript(testing, jsInstrumentationSettings) {
        const contentScriptConfig = {
            testing,
            jsInstrumentationSettings,
        };
        if (contentScriptConfig) {
            // TODO: Avoid using window to pass the content script config
            await browser.contentScripts.register({
                js: [
                    {
                        code: `window.openWpmContentScriptConfig = ${JSON.stringify(contentScriptConfig)};`,
                    },
                ],
                matches: ["<all_urls>"],
                allFrames: true,
                runAt: "document_start",
                matchAboutBlank: true,
            });
        }
        return browser.contentScripts.register({
            js: [{ file: "/content.js" }],
            matches: ["<all_urls>"],
            allFrames: true,
            runAt: "document_start",
            matchAboutBlank: true,
        });
    }
    cleanup() {
        this.pendingRecords = [];
        if (this.onMessageListener) {
            browser.runtime.onMessage.removeListener(this.onMessageListener);
        }
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiamF2YXNjcmlwdC1pbnN0cnVtZW50LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vc3JjL2JhY2tncm91bmQvamF2YXNjcmlwdC1pbnN0cnVtZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUNBLE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxNQUFNLHdDQUF3QyxDQUFDO0FBQ2pGLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxNQUFNLCtCQUErQixDQUFDO0FBQ3JFLE9BQU8sRUFBRSxTQUFTLEVBQUUsWUFBWSxFQUFFLFNBQVMsRUFBRSxNQUFNLHFCQUFxQixDQUFDO0FBSXpFLE1BQU0sT0FBTyxvQkFBb0I7SUFDL0I7Ozs7OztPQU1HO0lBQ0ssTUFBTSxDQUFDLHFCQUFxQixDQUFDLElBQUksRUFBRSxNQUFxQjtRQUM5RCxNQUFNLE1BQU0sR0FBRyxFQUF5QixDQUFDO1FBQ3pDLE1BQU0sQ0FBQyxzQkFBc0IsR0FBRyxvQkFBb0IsQ0FBQztRQUNyRCxNQUFNLENBQUMsYUFBYSxHQUFHLHVCQUF1QixFQUFFLENBQUM7UUFDakQsTUFBTSxDQUFDLHlCQUF5QixHQUFHLElBQUksQ0FBQyxPQUFPLENBQUM7UUFDaEQsTUFBTSxDQUFDLFNBQVMsR0FBRyxNQUFNLENBQUMsR0FBRyxDQUFDLFFBQVEsQ0FBQztRQUN2QyxNQUFNLENBQUMsTUFBTSxHQUFHLE1BQU0sQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDO1FBQzlCLE1BQU0sQ0FBQyxRQUFRLEdBQUcsTUFBTSxDQUFDLE9BQU8sQ0FBQztRQUNqQyxNQUFNLENBQUMsVUFBVSxHQUFHLFNBQVMsQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLENBQUM7UUFDOUMsTUFBTSxDQUFDLFdBQVcsR0FBRyxZQUFZLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDO1FBQ25ELE1BQU0sQ0FBQyxVQUFVLEdBQUcsWUFBWSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQztRQUNqRCxNQUFNLENBQUMsU0FBUyxHQUFHLFlBQVksQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUM7UUFDL0MsTUFBTSxDQUFDLGVBQWUsR0FBRyxZQUFZLENBQUMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxDQUFDO1FBQzFELE1BQU0sQ0FBQyxVQUFVLEdBQUcsWUFBWSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQztRQUNqRCxNQUFNLENBQUMsTUFBTSxHQUFHLFlBQVksQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLENBQUM7UUFDMUMsTUFBTSxDQUFDLFNBQVMsR0FBRyxZQUFZLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDO1FBQ2hELE1BQU0sQ0FBQyxLQUFLLEdBQUcsWUFBWSxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUN4QyxNQUFNLENBQUMsVUFBVSxHQUFHLFlBQVksQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLENBQUM7UUFDbEQsTUFBTSxDQUFDLFVBQVUsR0FBRyxJQUFJLENBQUMsU0FBUyxDQUFDO1FBQ25DLE1BQU0sQ0FBQyxTQUFTLEdBQUcsU0FBUyxDQUFDLE1BQU0sQ0FBQyxHQUFHLENBQUMsU0FBUyxDQUFDLENBQUM7UUFFbkQsb0RBQW9EO1FBQ3BELHVEQUF1RDtRQUN2RCxNQUFNLENBQUMsWUFBWSxHQUFHLFNBQVMsQ0FBQyxNQUFNLENBQUMsR0FBRyxDQUFDLENBQUM7UUFDNUMsTUFBTSxDQUFDLGFBQWEsR0FBRyxTQUFTLENBQUMsTUFBTSxDQUFDLEdBQUcsQ0FBQyxHQUFHLENBQUMsQ0FBQztRQUVqRCxJQUFJLElBQUksQ0FBQyxTQUFTLEtBQUssTUFBTSxJQUFJLElBQUksQ0FBQyxJQUFJLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRTtZQUNyRCxNQUFNLENBQUMsU0FBUyxHQUFHLFlBQVksQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDO1NBQzVEO1FBRUQsT0FBTyxNQUFNLENBQUM7SUFDaEIsQ0FBQztJQUNnQixZQUFZLENBQUM7SUFDdEIsaUJBQWlCLENBQUM7SUFDbEIsVUFBVSxHQUFZLEtBQUssQ0FBQztJQUM1QixjQUFjLEdBQTBCLEVBQUUsQ0FBQztJQUMzQyxPQUFPLENBQUM7SUFFaEIsWUFBWSxZQUFZO1FBQ3RCLElBQUksQ0FBQyxZQUFZLEdBQUcsWUFBWSxDQUFDO0lBQ25DLENBQUM7SUFFRDs7T0FFRztJQUNJLE1BQU07UUFDWCxJQUFJLENBQUMsaUJBQWlCLEdBQUcsQ0FBQyxPQUFPLEVBQUUsTUFBTSxFQUFFLEVBQUU7WUFDM0MsSUFDRSxPQUFPLENBQUMsU0FBUztnQkFDakIsT0FBTyxDQUFDLFNBQVMsS0FBSyw0QkFBNEIsRUFDbEQ7Z0JBQ0EsSUFBSSxDQUFDLDhCQUE4QixDQUFDLE9BQU8sRUFBRSxNQUFNLENBQUMsQ0FBQzthQUN0RDtRQUNILENBQUMsQ0FBQztRQUNGLE9BQU8sQ0FBQyxPQUFPLENBQUMsU0FBUyxDQUFDLFdBQVcsQ0FBQyxJQUFJLENBQUMsaUJBQWlCLENBQUMsQ0FBQztJQUNoRSxDQUFDO0lBRUQ7Ozs7OztPQU1HO0lBQ0ksOEJBQThCLENBQUMsT0FBTyxFQUFFLE1BQXFCO1FBQ2xFLFFBQVEsT0FBTyxDQUFDLElBQUksRUFBRTtZQUNwQixLQUFLLFNBQVMsQ0FBQztZQUNmLEtBQUssVUFBVTtnQkFDYixNQUFNLE1BQU0sR0FBRyxvQkFBb0IsQ0FBQyxxQkFBcUIsQ0FDdkQsT0FBTyxDQUFDLElBQUksRUFDWixNQUFNLENBQ1AsQ0FBQztnQkFDRixJQUFJLElBQUksQ0FBQyxVQUFVLEVBQUU7b0JBQ25CLE1BQU0sQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQztvQkFDakMsSUFBSSxDQUFDLFlBQVksQ0FBQyxVQUFVLENBQUMsWUFBWSxFQUFFLE1BQU0sQ0FBQyxDQUFDO2lCQUNwRDtxQkFBTTtvQkFDTCxJQUFJLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsQ0FBQztpQkFDbEM7Z0JBQ0QsTUFBTTtTQUNUO0lBQ0gsQ0FBQztJQUVEOzs7Ozs7T0FNRztJQUNJLEdBQUcsQ0FBQyxPQUFPO1FBQ2hCLElBQUksQ0FBQyxJQUFJLENBQUMsaUJBQWlCLEVBQUU7WUFDM0IsSUFBSSxDQUFDLE1BQU0sRUFBRSxDQUFDO1NBQ2Y7UUFDRCxJQUFJLENBQUMsT0FBTyxHQUFHLE9BQU8sQ0FBQztRQUN2QixJQUFJLENBQUMsVUFBVSxHQUFHLElBQUksQ0FBQztRQUN2QixJQUFJLENBQUMsY0FBYyxDQUFDLEdBQUcsQ0FBQyxDQUFDLE1BQU0sRUFBRSxFQUFFO1lBQ2pDLE1BQU0sQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQztZQUNqQyxJQUFJLENBQUMsWUFBWSxDQUFDLFVBQVUsQ0FBQyxZQUFZLEVBQUUsTUFBTSxDQUFDLENBQUM7UUFDckQsQ0FBQyxDQUFDLENBQUM7SUFDTCxDQUFDO0lBRU0sS0FBSyxDQUFDLHFCQUFxQixDQUNoQyxPQUFnQixFQUNoQix5QkFBZ0Q7UUFFaEQsTUFBTSxtQkFBbUIsR0FBRztZQUMxQixPQUFPO1lBQ1AseUJBQXlCO1NBQzFCLENBQUM7UUFDRixJQUFJLG1CQUFtQixFQUFFO1lBQ3ZCLDZEQUE2RDtZQUM3RCxNQUFNLE9BQU8sQ0FBQyxjQUFjLENBQUMsUUFBUSxDQUFDO2dCQUNwQyxFQUFFLEVBQUU7b0JBQ0Y7d0JBQ0UsSUFBSSxFQUFFLHVDQUF1QyxJQUFJLENBQUMsU0FBUyxDQUN6RCxtQkFBbUIsQ0FDcEIsR0FBRztxQkFDTDtpQkFDRjtnQkFDRCxPQUFPLEVBQUUsQ0FBQyxZQUFZLENBQUM7Z0JBQ3ZCLFNBQVMsRUFBRSxJQUFJO2dCQUNmLEtBQUssRUFBRSxnQkFBZ0I7Z0JBQ3ZCLGVBQWUsRUFBRSxJQUFJO2FBQ3RCLENBQUMsQ0FBQztTQUNKO1FBQ0QsT0FBTyxPQUFPLENBQUMsY0FBYyxDQUFDLFFBQVEsQ0FBQztZQUNyQyxFQUFFLEVBQUUsQ0FBQyxFQUFFLElBQUksRUFBRSxhQUFhLEVBQUUsQ0FBQztZQUM3QixPQUFPLEVBQUUsQ0FBQyxZQUFZLENBQUM7WUFDdkIsU0FBUyxFQUFFLElBQUk7WUFDZixLQUFLLEVBQUUsZ0JBQWdCO1lBQ3ZCLGVBQWUsRUFBRSxJQUFJO1NBQ3RCLENBQUMsQ0FBQztJQUNMLENBQUM7SUFFTSxPQUFPO1FBQ1osSUFBSSxDQUFDLGNBQWMsR0FBRyxFQUFFLENBQUM7UUFDekIsSUFBSSxJQUFJLENBQUMsaUJBQWlCLEVBQUU7WUFDMUIsT0FBTyxDQUFDLE9BQU8sQ0FBQyxTQUFTLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxpQkFBaUIsQ0FBQyxDQUFDO1NBQ2xFO0lBQ0gsQ0FBQztDQUNGIn0=

/***/ }),

/***/ "../webext-instrumentation/build/module/background/navigation-instrument.js":
/*!**********************************************************************************!*\
  !*** ../webext-instrumentation/build/module/background/navigation-instrument.js ***!
  \**********************************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "transformWebNavigationBaseEventDetailsToOpenWPMSchema": () => (/* binding */ transformWebNavigationBaseEventDetailsToOpenWPMSchema),
/* harmony export */   "NavigationInstrument": () => (/* binding */ NavigationInstrument)
/* harmony export */ });
/* harmony import */ var _lib_extension_session_event_ordinal__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ../lib/extension-session-event-ordinal */ "../webext-instrumentation/build/module/lib/extension-session-event-ordinal.js");
/* harmony import */ var _lib_extension_session_uuid__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ../lib/extension-session-uuid */ "../webext-instrumentation/build/module/lib/extension-session-uuid.js");
/* harmony import */ var _lib_pending_navigation__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! ../lib/pending-navigation */ "../webext-instrumentation/build/module/lib/pending-navigation.js");
/* harmony import */ var _lib_string_utils__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(/*! ../lib/string-utils */ "../webext-instrumentation/build/module/lib/string-utils.js");
/* harmony import */ var _lib_uuid__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(/*! ../lib/uuid */ "../webext-instrumentation/build/module/lib/uuid.js");





const transformWebNavigationBaseEventDetailsToOpenWPMSchema = async (crawlID, details) => {
    const tab = details.tabId > -1
        ? await browser.tabs.get(details.tabId)
        : {
            windowId: undefined,
            incognito: undefined,
            cookieStoreId: undefined,
            openerTabId: undefined,
            width: undefined,
            height: undefined,
        };
    const window = tab.windowId
        ? await browser.windows.get(tab.windowId)
        : { width: undefined, height: undefined, type: undefined };
    const navigation = {
        browser_id: crawlID,
        incognito: (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_3__.boolToInt)(tab.incognito),
        extension_session_uuid: _lib_extension_session_uuid__WEBPACK_IMPORTED_MODULE_1__.extensionSessionUuid,
        process_id: details.processId,
        window_id: tab.windowId,
        tab_id: details.tabId,
        tab_opener_tab_id: tab.openerTabId,
        frame_id: details.frameId,
        window_width: window.width,
        window_height: window.height,
        window_type: window.type,
        tab_width: tab.width,
        tab_height: tab.height,
        tab_cookie_store_id: (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_3__.escapeString)(tab.cookieStoreId),
        uuid: (0,_lib_uuid__WEBPACK_IMPORTED_MODULE_4__.makeUUID)(),
        url: (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_3__.escapeUrl)(details.url),
    };
    return navigation;
};
class NavigationInstrument {
    static navigationId(processId, tabId, frameId) {
        return `${processId}-${tabId}-${frameId}`;
    }
    dataReceiver;
    onBeforeNavigateListener;
    onCommittedListener;
    pendingNavigations = {};
    constructor(dataReceiver) {
        this.dataReceiver = dataReceiver;
    }
    run(crawlID) {
        this.onBeforeNavigateListener = async (details) => {
            const navigationId = NavigationInstrument.navigationId(details.processId, details.tabId, details.frameId);
            const pendingNavigation = this.instantiatePendingNavigation(navigationId);
            const navigation = await transformWebNavigationBaseEventDetailsToOpenWPMSchema(crawlID, details);
            navigation.parent_frame_id = details.parentFrameId;
            navigation.before_navigate_event_ordinal = (0,_lib_extension_session_event_ordinal__WEBPACK_IMPORTED_MODULE_0__.incrementedEventOrdinal)();
            navigation.before_navigate_time_stamp = new Date(details.timeStamp).toISOString();
            pendingNavigation.resolveOnBeforeNavigateEventNavigation(navigation);
        };
        browser.webNavigation.onBeforeNavigate.addListener(this.onBeforeNavigateListener);
        this.onCommittedListener = async (details) => {
            const navigationId = NavigationInstrument.navigationId(details.processId, details.tabId, details.frameId);
            const navigation = await transformWebNavigationBaseEventDetailsToOpenWPMSchema(crawlID, details);
            navigation.transition_qualifiers = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_3__.escapeString)(JSON.stringify(details.transitionQualifiers));
            navigation.transition_type = (0,_lib_string_utils__WEBPACK_IMPORTED_MODULE_3__.escapeString)(details.transitionType);
            navigation.committed_event_ordinal = (0,_lib_extension_session_event_ordinal__WEBPACK_IMPORTED_MODULE_0__.incrementedEventOrdinal)();
            navigation.committed_time_stamp = new Date(details.timeStamp).toISOString();
            // include attributes from the corresponding onBeforeNavigation event
            const pendingNavigation = this.getPendingNavigation(navigationId);
            if (pendingNavigation) {
                pendingNavigation.resolveOnCommittedEventNavigation(navigation);
                const resolved = await pendingNavigation.resolvedWithinTimeout(1000);
                if (resolved) {
                    const onBeforeNavigateEventNavigation = await pendingNavigation.onBeforeNavigateEventNavigation;
                    navigation.parent_frame_id =
                        onBeforeNavigateEventNavigation.parent_frame_id;
                    navigation.before_navigate_event_ordinal =
                        onBeforeNavigateEventNavigation.before_navigate_event_ordinal;
                    navigation.before_navigate_time_stamp =
                        onBeforeNavigateEventNavigation.before_navigate_time_stamp;
                }
            }
            this.dataReceiver.saveRecord("navigations", navigation);
        };
        browser.webNavigation.onCommitted.addListener(this.onCommittedListener);
    }
    cleanup() {
        if (this.onBeforeNavigateListener) {
            browser.webNavigation.onBeforeNavigate.removeListener(this.onBeforeNavigateListener);
        }
        if (this.onCommittedListener) {
            browser.webNavigation.onCommitted.removeListener(this.onCommittedListener);
        }
    }
    instantiatePendingNavigation(navigationId) {
        this.pendingNavigations[navigationId] = new _lib_pending_navigation__WEBPACK_IMPORTED_MODULE_2__.PendingNavigation();
        return this.pendingNavigations[navigationId];
    }
    getPendingNavigation(navigationId) {
        return this.pendingNavigations[navigationId];
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmF2aWdhdGlvbi1pbnN0cnVtZW50LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vc3JjL2JhY2tncm91bmQvbmF2aWdhdGlvbi1pbnN0cnVtZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBLE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxNQUFNLHdDQUF3QyxDQUFDO0FBQ2pGLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxNQUFNLCtCQUErQixDQUFDO0FBQ3JFLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLDJCQUEyQixDQUFDO0FBQzlELE9BQU8sRUFBRSxTQUFTLEVBQUUsWUFBWSxFQUFFLFNBQVMsRUFBRSxNQUFNLHFCQUFxQixDQUFDO0FBQ3pFLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxhQUFhLENBQUM7QUFRdkMsTUFBTSxDQUFDLE1BQU0scURBQXFELEdBQUcsS0FBSyxFQUN4RSxPQUFPLEVBQ1AsT0FBc0MsRUFDakIsRUFBRTtJQUN2QixNQUFNLEdBQUcsR0FDUCxPQUFPLENBQUMsS0FBSyxHQUFHLENBQUMsQ0FBQztRQUNoQixDQUFDLENBQUMsTUFBTSxPQUFPLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDO1FBQ3ZDLENBQUMsQ0FBQztZQUNFLFFBQVEsRUFBRSxTQUFTO1lBQ25CLFNBQVMsRUFBRSxTQUFTO1lBQ3BCLGFBQWEsRUFBRSxTQUFTO1lBQ3hCLFdBQVcsRUFBRSxTQUFTO1lBQ3RCLEtBQUssRUFBRSxTQUFTO1lBQ2hCLE1BQU0sRUFBRSxTQUFTO1NBQ2xCLENBQUM7SUFDUixNQUFNLE1BQU0sR0FBRyxHQUFHLENBQUMsUUFBUTtRQUN6QixDQUFDLENBQUMsTUFBTSxPQUFPLENBQUMsT0FBTyxDQUFDLEdBQUcsQ0FBQyxHQUFHLENBQUMsUUFBUSxDQUFDO1FBQ3pDLENBQUMsQ0FBQyxFQUFFLEtBQUssRUFBRSxTQUFTLEVBQUUsTUFBTSxFQUFFLFNBQVMsRUFBRSxJ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

/***/ }),

/***/ "../webext-instrumentation/build/module/content/javascript-instrument-content-scope.js":
/*!*********************************************************************************************!*\
  !*** ../webext-instrumentation/build/module/content/javascript-instrument-content-scope.js ***!
  \*********************************************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "injectJavascriptInstrumentPageScript": () => (/* binding */ injectJavascriptInstrumentPageScript)
/* harmony export */ });
/* harmony import */ var _lib_js_instruments__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ../lib/js-instruments */ "../webext-instrumentation/build/module/lib/js-instruments.js");
/* harmony import */ var _javascript_instrument_page_scope__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ./javascript-instrument-page-scope */ "../webext-instrumentation/build/module/content/javascript-instrument-page-scope.js");


function getPageScriptAsString(jsInstrumentationSettings) {
    // The JS Instrument Requests are setup and validated python side
    // including setting defaults for logSettings. See JSInstrumentation.py
    const pageScriptString = `
// Start of js-instruments.
${_lib_js_instruments__WEBPACK_IMPORTED_MODULE_0__.getInstrumentJS}
// End of js-instruments.

// Start of custom instrumentRequests.
const jsInstrumentationSettings = ${JSON.stringify(jsInstrumentationSettings)};
// End of custom instrumentRequests.

// Start of anonymous function from javascript-instrument-page-scope.ts
(${_javascript_instrument_page_scope__WEBPACK_IMPORTED_MODULE_1__.pageScript}(getInstrumentJS, jsInstrumentationSettings));
// End.
  `;
    return pageScriptString;
}
;
function insertScript(pageScriptString, eventId, testing = false) {
    const parent = document.documentElement;
    const script = document.createElement("script");
    script.text = pageScriptString;
    script.async = false;
    script.setAttribute("data-event-id", eventId);
    script.setAttribute("data-testing", `${testing}`);
    parent.insertBefore(script, parent.firstChild);
    parent.removeChild(script);
}
;
function emitMsg(type, msg) {
    msg.timeStamp = new Date().toISOString();
    browser.runtime.sendMessage({
        namespace: "javascript-instrumentation",
        type,
        data: msg,
    });
}
;
const eventId = Math.random().toString();
// listen for messages from the script we are about to insert
document.addEventListener(eventId, (e) => {
    // pass these on to the background page
    const msgs = e.detail;
    if (Array.isArray(msgs)) {
        msgs.forEach((msg) => {
            emitMsg(msg.type, msg.content);
        });
    }
    else {
        emitMsg(msgs.type, msgs.content);
    }
});
const injectJavascriptInstrumentPageScript = (contentScriptConfig) => {
    insertScript(getPageScriptAsString(contentScriptConfig.jsInstrumentationSettings), eventId, contentScriptConfig.testing);
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiamF2YXNjcmlwdC1pbnN0cnVtZW50LWNvbnRlbnQtc2NvcGUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi9zcmMvY29udGVudC9qYXZhc2NyaXB0LWluc3RydW1lbnQtY29udGVudC1zY29wZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQSxPQUFPLEVBQUUsZUFBZSxFQUF1QixNQUFNLHVCQUF1QixDQUFDO0FBQzdFLE9BQU8sRUFBRSxVQUFVLEVBQUUsTUFBTSxvQ0FBb0MsQ0FBQztBQUdoRSxTQUFTLHFCQUFxQixDQUM1Qix5QkFBZ0Q7SUFFaEQsaUVBQWlFO0lBQ2pFLHVFQUF1RTtJQUN2RSxNQUFNLGdCQUFnQixHQUFHOztFQUV6QixlQUFlOzs7O29DQUltQixJQUFJLENBQUMsU0FBUyxDQUFDLHlCQUF5QixDQUFDOzs7O0dBSTFFLFVBQVU7O0dBRVYsQ0FBQztJQUNGLE9BQU8sZ0JBQWdCLENBQUM7QUFDMUIsQ0FBQztBQUFBLENBQUM7QUFFRixTQUFTLFlBQVksQ0FDbkIsZ0JBQXdCLEVBQ3hCLE9BQWUsRUFDZixVQUFtQixLQUFLO0lBRXhCLE1BQU0sTUFBTSxHQUFHLFFBQVEsQ0FBQyxlQUFlLENBQUM7SUFDeEMsTUFBTSxNQUFNLEdBQUcsUUFBUSxDQUFDLGFBQWEsQ0FBQyxRQUFRLENBQUMsQ0FBQztJQUNoRCxNQUFNLENBQUMsSUFBSSxHQUFHLGdCQUFnQixDQUFDO0lBQy9CLE1BQU0sQ0FBQyxLQUFLLEdBQUcsS0FBSyxDQUFDO0lBQ3JCLE1BQU0sQ0FBQyxZQUFZLENBQUMsZUFBZSxFQUFFLE9BQU8sQ0FBQyxDQUFDO0lBQzlDLE1BQU0sQ0FBQyxZQUFZLENBQUMsY0FBYyxFQUFFLEdBQUcsT0FBTyxFQUFFLENBQUMsQ0FBQztJQUNsRCxNQUFNLENBQUMsWUFBWSxDQUFDLE1BQU0sRUFBRSxNQUFNLENBQUMsVUFBVSxDQUFDLENBQUM7SUFDL0MsTUFBTSxDQUFDLFdBQVcsQ0FBQyxNQUFNLENBQUMsQ0FBQztBQUM3QixDQUFDO0FBQUEsQ0FBQztBQUVGLFNBQVMsT0FBTyxDQUFFLElBQUksRUFBRSxHQUFHO0lBQ3pCLEdBQUcsQ0FBQyxTQUFTLEdBQUcsSUFBSSxJQUFJLEVBQUUsQ0FBQyxXQUFXLEVBQUUsQ0FBQztJQUN6QyxPQUFPLENBQUMsT0FBTyxDQUFDLFdBQVcsQ0FBQztRQUMxQixTQUFTLEVBQUUsNEJBQTRCO1FBQ3ZDLElBQUk7UUFDSixJQUFJLEVBQUUsR0FBRztLQUNWLENBQUMsQ0FBQztBQUNMLENBQUM7QUFBQSxDQUFDO0FBRUYsTUFBTSxPQUFPLEdBQUcsSUFBSSxDQUFDLE1BQU0sRUFBRSxDQUFDLFFBQVEsRUFBRSxDQUFDO0FBRXpDLDZEQUE2RDtBQUM3RCxRQUFRLENBQUMsZ0JBQWdCLENBQUMsT0FBTyxFQUFFLENBQUMsQ0FBYyxFQUFFLEVBQUU7SUFDcEQsdUNBQXVDO0lBQ3ZDLE1BQU0sSUFBSSxHQUFHLENBQUMsQ0FBQyxNQUFNLENBQUM7SUFDdEIsSUFBSSxLQUFLLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxFQUFFO1FBQ3ZCLElBQUksQ0FBQyxPQUFPLENBQUMsQ0FBQyxHQUFHLEVBQUUsRUFBRTtZQUNuQixPQUFPLENBQUMsR0FBRyxDQUFDLElBQUksRUFBRSxHQUFHLENBQUMsT0FBTyxDQUFDLENBQUM7UUFDakMsQ0FBQyxDQUFDLENBQUM7S0FDSjtTQUFNO1FBQ0wsT0FBTyxDQUFDLElBQUksQ0FBQyxJQUFJLEVBQUUsSUFBSSxDQUFDLE9BQU8sQ0FBQyxDQUFDO0tBQ2xDO0FBQ0gsQ0FBQyxDQUFDLENBQUM7QUFFSCxNQUFNLENBQUMsTUFBTSxvQ0FBb0MsR0FBRyxDQUFDLG1CQUErQyxFQUFFLEVBQUU7SUFDdEcsWUFBWSxDQUNWLHFCQUFxQixDQUFDLG1CQUFtQixDQUFDLHlCQUF5QixDQUFDLEVBQ3BFLE9BQU8sRUFDUCxtQkFBbUIsQ0FBQyxPQUFPLENBQzVCLENBQUM7QUFDSixDQUFDLENBQUEifQ==

/***/ }),

/***/ "../webext-instrumentation/build/module/content/javascript-instrument-page-scope.js":
/*!******************************************************************************************!*\
  !*** ../webext-instrumentation/build/module/content/javascript-instrument-page-scope.js ***!
  \******************************************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "pageScript": () => (/* binding */ pageScript)
/* harmony export */ });
/* eslint-disable no-console */
// Code below is not a content script: no Firefox APIs should be used
// Also, no webpack/es6 imports may be used in this file since the script
// is exported as a page script as a string
function pageScript(getInstrumentJS, jsInstrumentationSettings) {
    // messages the injected script
    const sendMessagesToLogger = (eventId, messages) => {
        document.dispatchEvent(new CustomEvent(eventId, {
            detail: messages,
        }));
    };
    const eventId = document.currentScript.getAttribute("data-event-id");
    const testing = document.currentScript.getAttribute("data-testing");
    const instrumentJS = getInstrumentJS(eventId, sendMessagesToLogger);
    let t0;
    if (testing === "true") {
        console.log("OpenWPM: Currently testing");
        t0 = performance.now();
        console.log("Begin loading JS instrumentation.");
    }
    instrumentJS(jsInstrumentationSettings);
    if (testing === "true") {
        const t1 = performance.now();
        console.log(`Call to instrumentJS took ${t1 - t0} milliseconds.`);
        window.instrumentJS = instrumentJS;
        console.log("OpenWPM: Content-side javascript instrumentation started with spec:", jsInstrumentationSettings, new Date().toISOString(), "(if spec is '<unavailable>' check web console.)");
    }
}
;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiamF2YXNjcmlwdC1pbnN0cnVtZW50LXBhZ2Utc2NvcGUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi9zcmMvY29udGVudC9qYXZhc2NyaXB0LWluc3RydW1lbnQtcGFnZS1zY29wZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQSwrQkFBK0I7QUFDL0IscUVBQXFFO0FBQ3JFLHlFQUF5RTtBQUN6RSwyQ0FBMkM7QUFFM0MsTUFBTSxVQUFVLFVBQVUsQ0FBRSxlQUFlLEVBQUUseUJBQXlCO0lBQ3BFLCtCQUErQjtJQUMvQixNQUFNLG9CQUFvQixHQUFHLENBQUMsT0FBTyxFQUFFLFFBQVEsRUFBRSxFQUFFO1FBQ2pELFFBQVEsQ0FBQyxhQUFhLENBQ3BCLElBQUksV0FBVyxDQUFDLE9BQU8sRUFBRTtZQUN2QixNQUFNLEVBQUUsUUFBUTtTQUNqQixDQUFDLENBQ0gsQ0FBQztJQUNKLENBQUMsQ0FBQztJQUVGLE1BQU0sT0FBTyxHQUFHLFFBQVEsQ0FBQyxhQUFhLENBQUMsWUFBWSxDQUFDLGVBQWUsQ0FBQyxDQUFDO0lBQ3JFLE1BQU0sT0FBTyxHQUFHLFFBQVEsQ0FBQyxhQUFhLENBQUMsWUFBWSxDQUFDLGNBQWMsQ0FBQyxDQUFDO0lBQ3BFLE1BQU0sWUFBWSxHQUFHLGVBQWUsQ0FBQyxPQUFPLEVBQUUsb0JBQW9CLENBQUMsQ0FBQztJQUNwRSxJQUFJLEVBQVUsQ0FBQztJQUNmLElBQUksT0FBTyxLQUFLLE1BQU0sRUFBRTtRQUN0QixPQUFPLENBQUMsR0FBRyxDQUFDLDRCQUE0QixDQUFDLENBQUM7UUFDMUMsRUFBRSxHQUFHLFdBQVcsQ0FBQyxHQUFHLEVBQUUsQ0FBQztRQUN2QixPQUFPLENBQUMsR0FBRyxDQUFDLG1DQUFtQyxDQUFDLENBQUM7S0FDbEQ7SUFDRCxZQUFZLENBQUMseUJBQXlCLENBQUMsQ0FBQztJQUN4QyxJQUFJLE9BQU8sS0FBSyxNQUFNLEVBQUU7UUFDdEIsTUFBTSxFQUFFLEdBQUcsV0FBVyxDQUFDLEdBQUcsRUFBRSxDQUFDO1FBQzdCLE9BQU8sQ0FBQyxHQUFHLENBQUMsNkJBQTZCLEVBQUUsR0FBRyxFQUFFLGdCQUFnQixDQUFDLENBQUM7UUFDakUsTUFBYyxDQUFDLFlBQVksR0FBRyxZQUFZLENBQUM7UUFDNUMsT0FBTyxDQUFDLEdBQUcsQ0FDVCxxRUFBcUUsRUFDckUseUJBQXlCLEVBQ3pCLElBQUksSUFBSSxFQUFFLENBQUMsV0FBVyxFQUFFLEVBQ3hCLGlEQUFpRCxDQUNsRCxDQUFDO0tBQ0g7QUFDSCxDQUFDO0FBQUEsQ0FBQyJ9

/***/ }),

/***/ "../webext-instrumentation/build/module/index.js":
/*!*******************************************************!*\
  !*** ../webext-instrumentation/build/module/index.js ***!
  \*******************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "CookieInstrument": () => (/* reexport safe */ _background_cookie_instrument__WEBPACK_IMPORTED_MODULE_0__.CookieInstrument),
/* harmony export */   "transformCookieObjectToMatchOpenWPMSchema": () => (/* reexport safe */ _background_cookie_instrument__WEBPACK_IMPORTED_MODULE_0__.transformCookieObjectToMatchOpenWPMSchema),
/* harmony export */   "DnsInstrument": () => (/* reexport safe */ _background_dns_instrument__WEBPACK_IMPORTED_MODULE_1__.DnsInstrument),
/* harmony export */   "HttpInstrument": () => (/* reexport safe */ _background_http_instrument__WEBPACK_IMPORTED_MODULE_2__.HttpInstrument),
/* harmony export */   "allTypes": () => (/* reexport safe */ _background_http_instrument__WEBPACK_IMPORTED_MODULE_2__.allTypes),
/* harmony export */   "JavascriptInstrument": () => (/* reexport safe */ _background_javascript_instrument__WEBPACK_IMPORTED_MODULE_3__.JavascriptInstrument),
/* harmony export */   "NavigationInstrument": () => (/* reexport safe */ _background_navigation_instrument__WEBPACK_IMPORTED_MODULE_4__.NavigationInstrument),
/* harmony export */   "transformWebNavigationBaseEventDetailsToOpenWPMSchema": () => (/* reexport safe */ _background_navigation_instrument__WEBPACK_IMPORTED_MODULE_4__.transformWebNavigationBaseEventDetailsToOpenWPMSchema),
/* harmony export */   "injectJavascriptInstrumentPageScript": () => (/* reexport safe */ _content_javascript_instrument_content_scope__WEBPACK_IMPORTED_MODULE_5__.injectJavascriptInstrumentPageScript),
/* harmony export */   "HttpPostParser": () => (/* reexport safe */ _lib_http_post_parser__WEBPACK_IMPORTED_MODULE_6__.HttpPostParser),
/* harmony export */   "Uint8ToBase64": () => (/* reexport safe */ _lib_string_utils__WEBPACK_IMPORTED_MODULE_7__.Uint8ToBase64),
/* harmony export */   "boolToInt": () => (/* reexport safe */ _lib_string_utils__WEBPACK_IMPORTED_MODULE_7__.boolToInt),
/* harmony export */   "encode_utf8": () => (/* reexport safe */ _lib_string_utils__WEBPACK_IMPORTED_MODULE_7__.encode_utf8),
/* harmony export */   "escapeString": () => (/* reexport safe */ _lib_string_utils__WEBPACK_IMPORTED_MODULE_7__.escapeString),
/* harmony export */   "escapeUrl": () => (/* reexport safe */ _lib_string_utils__WEBPACK_IMPORTED_MODULE_7__.escapeUrl),
/* harmony export */   "dateTimeUnicodeFormatString": () => (/* reexport safe */ _schema__WEBPACK_IMPORTED_MODULE_8__.dateTimeUnicodeFormatString)
/* harmony export */ });
/* harmony import */ var _background_cookie_instrument__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ./background/cookie-instrument */ "../webext-instrumentation/build/module/background/cookie-instrument.js");
/* harmony import */ var _background_dns_instrument__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ./background/dns-instrument */ "../webext-instrumentation/build/module/background/dns-instrument.js");
/* harmony import */ var _background_http_instrument__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! ./background/http-instrument */ "../webext-instrumentation/build/module/background/http-instrument.js");
/* harmony import */ var _background_javascript_instrument__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(/*! ./background/javascript-instrument */ "../webext-instrumentation/build/module/background/javascript-instrument.js");
/* harmony import */ var _background_navigation_instrument__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(/*! ./background/navigation-instrument */ "../webext-instrumentation/build/module/background/navigation-instrument.js");
/* harmony import */ var _content_javascript_instrument_content_scope__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(/*! ./content/javascript-instrument-content-scope */ "../webext-instrumentation/build/module/content/javascript-instrument-content-scope.js");
/* harmony import */ var _lib_http_post_parser__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(/*! ./lib/http-post-parser */ "../webext-instrumentation/build/module/lib/http-post-parser.js");
/* harmony import */ var _lib_string_utils__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(/*! ./lib/string-utils */ "../webext-instrumentation/build/module/lib/string-utils.js");
/* harmony import */ var _schema__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__(/*! ./schema */ "../webext-instrumentation/build/module/schema.js");









//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi9zcmMvaW5kZXgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUEsY0FBYyxnQ0FBZ0MsQ0FBQztBQUMvQyxjQUFjLDZCQUE2QixDQUFDO0FBQzVDLGNBQWMsOEJBQThCLENBQUM7QUFDN0MsY0FBYyxvQ0FBb0MsQ0FBQztBQUNuRCxjQUFjLG9DQUFvQyxDQUFDO0FBQ25ELGNBQWMsK0NBQStDLENBQUM7QUFDOUQsY0FBYyx3QkFBd0IsQ0FBQztBQUN2QyxjQUFjLG9CQUFvQixDQUFDO0FBQ25DLGNBQWMsVUFBVSxDQUFDIn0=

/***/ }),

/***/ "../webext-instrumentation/build/module/lib/extension-session-event-ordinal.js":
/*!*************************************************************************************!*\
  !*** ../webext-instrumentation/build/module/lib/extension-session-event-ordinal.js ***!
  \*************************************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "incrementedEventOrdinal": () => (/* binding */ incrementedEventOrdinal)
/* harmony export */ });
/**
 * This enables us to keep information about the original order
 * in which events arrived to our event listeners.
 */
let eventOrdinal = 0;
const incrementedEventOrdinal = () => {
    return eventOrdinal++;
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXh0ZW5zaW9uLXNlc3Npb24tZXZlbnQtb3JkaW5hbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uL3NyYy9saWIvZXh0ZW5zaW9uLXNlc3Npb24tZXZlbnQtb3JkaW5hbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7O0dBR0c7QUFDSCxJQUFJLFlBQVksR0FBRyxDQUFDLENBQUM7QUFFckIsTUFBTSxDQUFDLE1BQU0sdUJBQXVCLEdBQUcsR0FBRyxFQUFFO0lBQzFDLE9BQU8sWUFBWSxFQUFFLENBQUM7QUFDeEIsQ0FBQyxDQUFDIn0=

/***/ }),

/***/ "../webext-instrumentation/build/module/lib/extension-session-uuid.js":
/*!****************************************************************************!*\
  !*** ../webext-instrumentation/build/module/lib/extension-session-uuid.js ***!
  \****************************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "extensionSessionUuid": () => (/* binding */ extensionSessionUuid)
/* harmony export */ });
/* harmony import */ var _uuid__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ./uuid */ "../webext-instrumentation/build/module/lib/uuid.js");

/**
 * This enables us to access a unique reference to this browser
 * session - regenerated any time the background process gets
 * restarted (which should only be on browser restarts)
 */
const extensionSessionUuid = (0,_uuid__WEBPACK_IMPORTED_MODULE_0__.makeUUID)();
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXh0ZW5zaW9uLXNlc3Npb24tdXVpZC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uL3NyYy9saWIvZXh0ZW5zaW9uLXNlc3Npb24tdXVpZC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQSxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sUUFBUSxDQUFDO0FBRWxDOzs7O0dBSUc7QUFDSCxNQUFNLENBQUMsTUFBTSxvQkFBb0IsR0FBRyxRQUFRLEVBQUUsQ0FBQyJ9

/***/ }),

/***/ "../webext-instrumentation/build/module/lib/http-post-parser.js":
/*!**********************************************************************!*\
  !*** ../webext-instrumentation/build/module/lib/http-post-parser.js ***!
  \**********************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "HttpPostParser": () => (/* binding */ HttpPostParser)
/* harmony export */ });
/* harmony import */ var _string_utils__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ./string-utils */ "../webext-instrumentation/build/module/lib/string-utils.js");

class HttpPostParser {
    onBeforeRequestEventDetails;
    dataReceiver;
    constructor(onBeforeRequestEventDetails, dataReceiver) {
        this.onBeforeRequestEventDetails = onBeforeRequestEventDetails;
        this.dataReceiver = dataReceiver;
    }
    parsePostRequest() {
        const requestBody = this.onBeforeRequestEventDetails.requestBody;
        if (requestBody.error) {
            this.dataReceiver.logError("Exception: Upstream failed to parse POST: " + requestBody.error);
        }
        if (requestBody.formData) {
            return {
                post_body: (0,_string_utils__WEBPACK_IMPORTED_MODULE_0__.escapeString)(JSON.stringify(requestBody.formData)),
            };
        }
        if (requestBody.raw) {
            return {
                post_body_raw: JSON.stringify(requestBody.raw.map((x) => [
                    x.file,
                    (0,_string_utils__WEBPACK_IMPORTED_MODULE_0__.Uint8ToBase64)(new Uint8Array(x.bytes)),
                ])),
            };
        }
        return {};
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaHR0cC1wb3N0LXBhcnNlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uL3NyYy9saWIvaHR0cC1wb3N0LXBhcnNlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFDQSxPQUFPLEVBQUUsWUFBWSxFQUFFLGFBQWEsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBUTdELE1BQU0sT0FBTyxjQUFjO0lBQ1IsMkJBQTJCLENBQXdDO0lBQ25FLFlBQVksQ0FBQztJQUU5QixZQUNFLDJCQUFrRSxFQUNsRSxZQUFZO1FBRVosSUFBSSxDQUFDLDJCQUEyQixHQUFHLDJCQUEyQixDQUFDO1FBQy9ELElBQUksQ0FBQyxZQUFZLEdBQUcsWUFBWSxDQUFDO0lBQ25DLENBQUM7SUFFTSxnQkFBZ0I7UUFDckIsTUFBTSxXQUFXLEdBQUcsSUFBSSxDQUFDLDJCQUEyQixDQUFDLFdBQVcsQ0FBQztRQUNqRSxJQUFJLFdBQVcsQ0FBQyxLQUFLLEVBQUU7WUFDckIsSUFBSSxDQUFDLFlBQVksQ0FBQyxRQUFRLENBQ3hCLDRDQUE0QyxHQUFHLFdBQVcsQ0FBQyxLQUFLLENBQ2pFLENBQUM7U0FDSDtRQUNELElBQUksV0FBVyxDQUFDLFFBQVEsRUFBRTtZQUN4QixPQUFPO2dCQUNMLFNBQVMsRUFBRSxZQUFZLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxXQUFXLENBQUMsUUFBUSxDQUFDLENBQUM7YUFDOUQsQ0FBQztTQUNIO1FBQ0QsSUFBSSxXQUFXLENBQUMsR0FBRyxFQUFFO1lBQ25CLE9BQU87Z0JBQ0wsYUFBYSxFQUFFLElBQUksQ0FBQyxTQUFTLENBQzNCLFdBQVcsQ0FBQyxHQUFHLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxFQUFFLEVBQUUsQ0FBQztvQkFDekIsQ0FBQyxDQUFDLElBQUk7b0JBQ04sYUFBYSxDQUFDLElBQUksVUFBVSxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUMsQ0FBQztpQkFDdkMsQ0FBQyxDQUNIO2FBQ0YsQ0FBQztTQUNIO1FBQ0QsT0FBTyxFQUFFLENBQUM7SUFDWixDQUFDO0NBQ0YifQ==

/***/ }),

/***/ "../webext-instrumentation/build/module/lib/js-instruments.js":
/*!********************************************************************!*\
  !*** ../webext-instrumentation/build/module/lib/js-instruments.js ***!
  \********************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "getInstrumentJS": () => (/* binding */ getInstrumentJS)
/* harmony export */ });
// Intrumentation injection code is based on privacybadgerfirefox
// https://github.com/EFForg/privacybadgerfirefox/blob/master/data/fingerprinting.js
function getInstrumentJS(eventId, sendMessagesToLogger) {
    /*
     * Instrumentation helpers
     * (Inlined in order for jsInstruments to be easily exportable as a string)
     */
    // function getAttributes (attributes) {
    //     return Array.from(attributes)
    //       .map(a => [a.name, a.value])
    //       .reduce((acc, attr) => {
    //         acc[attr[0]] = attr[1]
    //         return acc
    //       }, {})
    //   }
    // Counter to cap # of calls logged for each script/api combination
    const maxLogCount = 500;
    // logCounter
    const logCounter = new Object();
    // Prevent logging of gets arising from logging
    let inLog = false;
    // To keep track of the original order of events
    let ordinal = 0;
    // Options for JSOperation
    const JSOperation = {
        call: "call",
        get: "get",
        get_failed: "get(failed)",
        get_function: "get(function)",
        set: "set",
        set_failed: "set(failed)",
        set_prevented: "set(prevented)",
    };
    // Rough implementations of Object.getPropertyDescriptor and Object.getPropertyNames
    // See http://wiki.ecmascript.org/doku.php?id=harmony:extended_object_api
    Object.getPropertyDescriptor = function (subject, name) {
        if (subject === undefined) {
            throw new Error("Can't get property descriptor for undefined");
        }
        let pd = Object.getOwnPropertyDescriptor(subject, name);
        let proto = Object.getPrototypeOf(subject);
        while (pd === undefined && proto !== null) {
            pd = Object.getOwnPropertyDescriptor(proto, name);
            proto = Object.getPrototypeOf(proto);
        }
        return pd;
    };
    Object.getPropertyNames = function (subject) {
        if (subject === undefined) {
            throw new Error("Can't get property names for undefined");
        }
        let props = Object.getOwnPropertyNames(subject);
        let proto = Object.getPrototypeOf(subject);
        while (proto !== null) {
            props = props.concat(Object.getOwnPropertyNames(proto));
            proto = Object.getPrototypeOf(proto);
        }
        // FIXME: remove duplicate property names from props
        return props;
    };
    // debounce - from Underscore v1.6.0
    function debounce(func, wait, immediate = false) {
        let timeout;
        let args;
        let context;
        let timestamp;
        let result;
        const later = function () {
            const last = Date.now() - timestamp;
            if (last < wait) {
                timeout = setTimeout(later, wait - last);
            }
            else {
                timeout = null;
                if (!immediate) {
                    result = func.apply(context, args);
                    context = args = null;
                }
            }
        };
        return function () {
            context = this;
            args = arguments;
            timestamp = Date.now();
            const callNow = immediate && !timeout;
            if (!timeout) {
                timeout = setTimeout(later, wait);
            }
            if (callNow) {
                result = func.apply(context, args);
                context = args = null;
            }
            return result;
        };
    }
    // Recursively generates a path for an element
    function getPathToDomElement(element, visibilityAttr = false) {
        if (element === document.body) {
            return element.tagName;
        }
        if (element.parentNode === null) {
            return "NULL/" + element.tagName;
        }
        let siblingIndex = 1;
        const siblings = element.parentNode.childNodes;
        for (let i = 0; i < siblings.length; i++) {
            const sibling = siblings[i];
            if (sibling === element) {
                let path = getPathToDomElement(element.parentNode, visibilityAttr);
                path += "/" + element.tagName + "[" + siblingIndex;
                path += "," + element.id;
                path += "," + element.className;
                if (visibilityAttr) {
                    path += "," + element.hidden;
                    path += "," + element.style.display;
                    path += "," + element.style.visibility;
                }
                if (element.tagName === "A") {
                    path += "," + element.href;
                }
                path += "]";
                return path;
            }
            if (sibling.nodeType === 1 && sibling.tagName === element.tagName) {
                siblingIndex++;
            }
        }
    }
    // Helper for JSONifying objects
    function serializeObject(object, stringifyFunctions = false) {
        // Handle permissions errors
        try {
            if (object === null) {
                return "null";
            }
            if (typeof object === "function") {
                return stringifyFunctions ? object.toString() : "FUNCTION";
            }
            if (typeof object !== "object") {
                return object;
            }
            const seenObjects = [];
            return JSON.stringify(object, function (key, value) {
                if (value === null) {
                    return "null";
                }
                if (typeof value === "function") {
                    return stringifyFunctions ? value.toString() : "FUNCTION";
                }
                if (typeof value === "object") {
                    // Remove wrapping on content objects
                    if ("wrappedJSObject" in value) {
                        value = value.wrappedJSObject;
                    }
                    // Serialize DOM elements
                    if (value instanceof HTMLElement) {
                        return getPathToDomElement(value);
                    }
                    // Prevent serialization cycles
                    if (key === "" || seenObjects.indexOf(value) < 0) {
                        seenObjects.push(value);
                        return value;
                    }
                    else {
                        return typeof value;
                    }
                }
                return value;
            });
        }
        catch (error) {
            console.log("OpenWPM: SERIALIZATION ERROR: " + error);
            return "SERIALIZATION ERROR: " + error;
        }
    }
    function updateCounterAndCheckIfOver(scriptUrl, symbol) {
        const key = scriptUrl + "|" + symbol;
        if (key in logCounter && logCounter[key] >= maxLogCount) {
            return true;
        }
        else if (!(key in logCounter)) {
            logCounter[key] = 1;
        }
        else {
            logCounter[key] += 1;
        }
        return false;
    }
    // For gets, sets, etc. on a single value
    function logValue(instrumentedVariableName, value, attributes, operation, // from JSOperation object please
    callContext, logSettings) {
        if (inLog) {
            return;
        }
        inLog = true;
        const overLimit = updateCounterAndCheckIfOver(callContext.scriptUrl, instrumentedVariableName);
        if (overLimit) {
            inLog = false;
            return;
        }
        //attributes is a NamedNodeMap. Doing a conversion, taking only names and values.
        //Doing this only for src calls for now.
        if (instrumentedVariableName.includes('Element.src')) {
            var completeUrl = new URL(value, window.location.href);
            value = completeUrl.href;
            var newObject = Object.assign({}, Array.from(attributes, ({ name, value }) => ({ [name]: value })));
            attributes = newObject;
        }
        const msg = {
            operation,
            symbol: instrumentedVariableName,
            value: serializeObject(value, logSettings.logFunctionsAsStrings),
            attributes: serializeObject(attributes),
            scriptUrl: callContext.scriptUrl,
            scriptLine: callContext.scriptLine,
            scriptCol: callContext.scriptCol,
            funcName: callContext.funcName,
            scriptLocEval: callContext.scriptLocEval,
            callStack: callContext.callStack,
            ordinal: ordinal++,
        };
        try {
            send("logValue", msg);
        }
        catch (error) {
            console.log("OpenWPM: Unsuccessful value log!");
            logErrorToConsole(error);
        }
        inLog = false;
    }
    // For functions
    function logCall(instrumentedFunctionName, args, attributes, callContext, logSettings) {
        if (inLog) {
            return;
        }
        inLog = true;
        const overLimit = updateCounterAndCheckIfOver(callContext.scriptUrl, instrumentedFunctionName);
        if (overLimit) {
            inLog = false;
            return;
        }
        try {
            // Convert special arguments array to a standard array for JSONifying
            const serialArgs = [];
            for (const arg of args) {
                serialArgs.push(serializeObject(arg, logSettings.logFunctionsAsStrings));
            }
            if (instrumentedFunctionName === 'window.document.createElement') {
                var newObject = Object.assign({}, Array.from(attributes, ({ name, value }) => ({ [name]: value })));
                attributes = newObject;
            }
            const msg = {
                operation: JSOperation.call,
                symbol: instrumentedFunctionName,
                args: serialArgs,
                value: "",
                attributes: serializeObject(attributes),
                scriptUrl: callContext.scriptUrl,
                scriptLine: callContext.scriptLine,
                scriptCol: callContext.scriptCol,
                funcName: callContext.funcName,
                scriptLocEval: callContext.scriptLocEval,
                callStack: callContext.callStack,
                ordinal: ordinal++,
            };
            send("logCall", msg);
        }
        catch (error) {
            console.log("OpenWPM: Unsuccessful call log: " + instrumentedFunctionName);
            logErrorToConsole(error);
        }
        inLog = false;
    }
    function logErrorToConsole(error, context = false) {
        console.error("OpenWPM: Error name: " + error.name);
        console.error("OpenWPM: Error message: " + error.message);
        console.error("OpenWPM: Error filename: " + error.fileName);
        console.error("OpenWPM: Error line number: " + error.lineNumber);
        console.error("OpenWPM: Error stack: " + error.stack);
        if (context) {
            console.error("OpenWPM: Error context: " + JSON.stringify(context));
        }
    }
    // Helper to get originating script urls
    function getStackTrace() {
        let stack;
        try {
            throw new Error();
        }
        catch (err) {
            stack = err.stack;
        }
        return stack;
    }
    // from http://stackoverflow.com/a/5202185
    const rsplit = function (source, sep, maxsplit) {
        const split = source.split(sep);
        return maxsplit
            ? [split.slice(0, -maxsplit).join(sep)].concat(split.slice(-maxsplit))
            : split;
    };
    function getOriginatingScriptContext(getCallStack = false) {
        const trace = getStackTrace().trim().split("\n");
        // return a context object even if there is an error
        const empty_context = {
            scriptUrl: "",
            scriptLine: "",
            scriptCol: "",
            funcName: "",
            scriptLocEval: "",
            callStack: "",
        };
        if (trace.length < 4) {
            return empty_context;
        }
        // 0, 1 and 2 are OpenWPM's own functions (e.g. getStackTrace), skip them.
        const callSite = trace[3];
        if (!callSite) {
            return empty_context;
        }
        /*
         * Stack frame format is simply: FUNC_NAME@FILENAME:LINE_NO:COLUMN_NO
         *
         * If eval or Function is involved we have an additional part after the FILENAME, e.g.:
         * FUNC_NAME@FILENAME line 123 > eval line 1 > eval:LINE_NO:COLUMN_NO
         * or FUNC_NAME@FILENAME line 234 > Function:LINE_NO:COLUMN_NO
         *
         * We store the part between the FILENAME and the LINE_NO in scriptLocEval
         */
        try {
            let scriptUrl = "";
            let scriptLocEval = ""; // for eval or Function calls
            const callSiteParts = callSite.split("@");
            const funcName = callSiteParts[0] || "";
            const items = rsplit(callSiteParts[1], ":", 2);
            const columnNo = items[items.length - 1];
            const lineNo = items[items.length - 2];
            const scriptFileName = items[items.length - 3] || "";
            const lineNoIdx = scriptFileName.indexOf(" line "); // line in the URL means eval or Function
            if (lineNoIdx === -1) {
                scriptUrl = scriptFileName; // TODO: sometimes we have filename only, e.g. XX.js
            }
            else {
                scriptUrl = scriptFileName.slice(0, lineNoIdx);
                scriptLocEval = scriptFileName.slice(lineNoIdx + 1, scriptFileName.length);
            }
            const callContext = {
                scriptUrl,
                scriptLine: lineNo,
                scriptCol: columnNo,
                funcName,
                scriptLocEval,
                callStack: getCallStack ? trace.slice(3).join("\n").trim() : "",
            };
            return callContext;
        }
        catch (e) {
            console.log("OpenWPM: Error parsing the script context", e.toString(), callSite);
            return empty_context;
        }
    }
    function isObject(object, propertyName) {
        let property;
        try {
            property = object[propertyName];
        }
        catch (error) {
            return false;
        }
        if (property === null) {
            // null is type "object"
            return false;
        }
        return typeof property === "object";
    }
    // Log calls to a given function
    // This helper function returns a wrapper around `func` which logs calls
    // to `func`. `objectName` and `methodName` are used strictly to identify
    // which object method `func` is coming from in the logs
    function instrumentFunction(objectName, methodName, func, logSettings) {
        return function () {
            const callContext = getOriginatingScriptContext(logSettings.logCallStack);
            /*********************/
            var attributes = "";
            // Check for creation of element. Set an openwpm attribute to identify this
            // element later. 
            if (methodName == "createElement") {
                //var observer = createMutationObserver(object, objectName, logSettings);
                // object.addEventListener("DOMAttrModified", function (event) {
                // console.log("in modify listener!");
                // switch(event.attrChange) {
                //   case MutationEvent.MODIFICATION:
                //     console.log("modified!");
                //     var node = event.target.tagName;
                //     logCall(node + ".attr_modified", "", event.target.attributes, callContext, logSettings);
                //     break;
                //   case MutationEvent.ADDITION:
                //     console.log("added!");
                //     logCall(node + ".attr_added", "", event.target.attributes, callContext, logSettings);
                //     break;
                //   case MutationEvent.REMOVAL:
                //     console.log("removed!");
                //     logCall(node + ".attr_removed", "", event.target.attributes, callContext, logSettings);
                //     break;
                //   }
                // }, false);
                var funcRef = func.apply(this, arguments);
                //Setting a random 5-digit tag for now.
                var tag = Math.floor(Math.random() * Math.pow(10, 5));
                funcRef.setAttribute("openwpm", tag);
                attributes = funcRef.attributes;
                logCall(objectName + '.' + methodName, arguments, attributes, callContext, logSettings);
                return funcRef;
            }
            /*********************/
            logCall(objectName + "." + methodName, arguments, attributes, callContext, logSettings);
            return func.apply(this, arguments);
        };
    }
    // Log properties of prototypes and objects
    function instrumentObjectProperty(object, objectName, propertyName, logSettings) {
        if (!object ||
            !objectName ||
            !propertyName ||
            propertyName === "undefined") {
            throw new Error(`Invalid request to instrumentObjectProperty.
        Object: ${object}
        objectName: ${objectName}
        propertyName: ${propertyName}
        `);
        }
        // Store original descriptor in closure
        const propDesc = Object.getPropertyDescriptor(object, propertyName);
        // Property descriptor must exist unless we are instrumenting a nonExisting property
        if (!propDesc &&
            !logSettings.nonExistingPropertiesToInstrument.includes(propertyName)) {
            console.error("Property descriptor not found for", objectName, propertyName, object);
            return;
        }
        // Property descriptor for undefined properties
        let undefinedPropValue;
        const undefinedPropDesc = {
            get: () => {
                return undefinedPropValue;
            },
            set: (value) => {
                undefinedPropValue = value;
            },
            enumerable: false,
        };
        // Instrument data or accessor property descriptors
        const originalGetter = propDesc ? propDesc.get : undefinedPropDesc.get;
        const originalSetter = propDesc ? propDesc.set : undefinedPropDesc.set;
        let originalValue = propDesc ? propDesc.value : undefinedPropValue;
        // We overwrite both data and accessor properties as an instrumented
        // accessor property
        Object.defineProperty(object, propertyName, {
            configurable: true,
            get: (function () {
                return function () {
                    let origProperty;
                    const callContext = getOriginatingScriptContext(logSettings.logCallStack);
                    const instrumentedVariableName = `${objectName}.${propertyName}`;
                    var attributes = "";
                    // get original value
                    if (!propDesc) {
                        // if undefined property
                        origProperty = undefinedPropValue;
                    }
                    else if (originalGetter) {
                        // if accessor property
                        origProperty = originalGetter.call(this);
                    }
                    else if ("value" in propDesc) {
                        // if data property
                        origProperty = originalValue;
                    }
                    else {
                        console.error(`Property descriptor for ${instrumentedVariableName} doesn't have getter or value?`);
                        logValue(instrumentedVariableName, "", attributes, JSOperation.get_failed, callContext, logSettings);
                        return;
                    }
                    // Log `gets` except those that have instrumented return values
                    // * All returned functions are instrumented with a wrapper
                    // * Returned objects may be instrumented if recursive
                    //   instrumentation is enabled and this isn't at the depth limit.
                    if (typeof origProperty === "function") {
                        if (logSettings.logFunctionGets) {
                            logValue(instrumentedVariableName, origProperty, attributes, JSOperation.get_function, callContext, logSettings);
                        }
                        const instrumentedFunctionWrapper = instrumentFunction(objectName, propertyName, origProperty, logSettings);
                        // Restore the original prototype and constructor so that instrumented classes remain intact
                        // TODO: This may have introduced prototype pollution as per https://github.com/mozilla/OpenWPM/issues/471
                        if (origProperty.prototype) {
                            instrumentedFunctionWrapper.prototype = origProperty.prototype;
                            if (origProperty.prototype.constructor) {
                                instrumentedFunctionWrapper.prototype.constructor =
                                    origProperty.prototype.constructor;
                            }
                        }
                        return instrumentedFunctionWrapper;
                    }
                    else if (typeof origProperty === "object" &&
                        logSettings.recursive &&
                        logSettings.depth > 0) {
                        return origProperty;
                    }
                    else {
                        logValue(instrumentedVariableName, origProperty, attributes, JSOperation.get, callContext, logSettings);
                        return origProperty;
                    }
                };
            })(),
            set: (function () {
                return function (value) {
                    const callContext = getOriginatingScriptContext(logSettings.logCallStack);
                    const instrumentedVariableName = `${objectName}.${propertyName}`;
                    let returnValue;
                    var attributes = "";
                    // Prevent sets for functions and objects if enabled
                    if (logSettings.preventSets &&
                        (typeof originalValue === "function" ||
                            typeof originalValue === "object")) {
                        logValue(instrumentedVariableName, value, attributes, JSOperation.set_prevented, callContext, logSettings);
                        return value;
                    }
                    // set new value to original setter/location
                    if (originalSetter) {
                        // if accessor property
                        returnValue = originalSetter.call(this, value);
                        try {
                            if (this.getAttribute("openwpm")) {
                                attributes = this.attributes;
                            }
                        }
                        catch (error) {
                            console.warn("Error in getting openwpm attribute");
                            attributes = "";
                        }
                    }
                    else if ("value" in propDesc) {
                        inLog = true;
                        if (object.isPrototypeOf(this)) {
                            Object.defineProperty(this, propertyName, {
                                value,
                            });
                        }
                        else {
                            originalValue = value;
                        }
                        returnValue = value;
                        inLog = false;
                    }
                    else {
                        console.error(`Property descriptor for ${instrumentedVariableName} doesn't have setter or value?`);
                        logValue(instrumentedVariableName, value, attributes, JSOperation.set_failed, callContext, logSettings);
                        return value;
                    }
                    logValue(instrumentedVariableName, value, attributes, JSOperation.set, callContext, logSettings);
                    return returnValue;
                };
            })(),
        });
    }
    function instrumentObject(object, instrumentedName, logSettings) {
        // Set propertiesToInstrument to null to force no properties to be instrumented.
        // (this is used in testing for example)
        let propertiesToInstrument;
        if (logSettings.propertiesToInstrument === null) {
            propertiesToInstrument = [];
        }
        else if (logSettings.propertiesToInstrument.length === 0) {
            propertiesToInstrument = Object.getPropertyNames(object);
        }
        else {
            propertiesToInstrument = logSettings.propertiesToInstrument;
        }
        for (const propertyName of propertiesToInstrument) {
            if (logSettings.excludedProperties.includes(propertyName)) {
                continue;
            }
            // If `recursive` flag set we want to recursively instrument any
            // object properties that aren't the prototype object.
            if (logSettings.recursive &&
                logSettings.depth > 0 &&
                isObject(object, propertyName) &&
                propertyName !== "__proto__") {
                const newInstrumentedName = `${instrumentedName}.${propertyName}`;
                const newLogSettings = { ...logSettings };
                newLogSettings.depth = logSettings.depth - 1;
                newLogSettings.propertiesToInstrument = [];
                instrumentObject(object[propertyName], newInstrumentedName, newLogSettings);
            }
            try {
                instrumentObjectProperty(object, instrumentedName, propertyName, logSettings);
            }
            catch (error) {
                if (error instanceof TypeError &&
                    error.message.includes("can't redefine non-configurable property")) {
                    console.warn(`Cannot instrument non-configurable property: ${instrumentedName}:${propertyName}`);
                }
                else {
                    logErrorToConsole(error, { instrumentedName, propertyName });
                }
            }
        }
        for (const propertyName of logSettings.nonExistingPropertiesToInstrument) {
            if (logSettings.excludedProperties.includes(propertyName)) {
                continue;
            }
            try {
                instrumentObjectProperty(object, instrumentedName, propertyName, logSettings);
            }
            catch (error) {
                logErrorToConsole(error, { instrumentedName, propertyName });
            }
        }
    }
    const sendFactory = function (eventId, $sendMessagesToLogger) {
        let messages = [];
        // debounce sending queued messages
        const _send = debounce(function () {
            $sendMessagesToLogger(eventId, messages);
            // clear the queue
            messages = [];
        }, 100);
        return function (msgType, msg) {
            // queue the message
            messages.push({ type: msgType, content: msg });
            _send();
        };
    };
    const send = sendFactory(eventId, sendMessagesToLogger);
    function instrumentJS(JSInstrumentRequests) {
        // The JS Instrument Requests are setup and validated python side
        // including setting defaults for logSettings.
        // More details about how this function is invoked are in
        // content/javascript-instrument-content-scope.ts
        JSInstrumentRequests.forEach(function (item) {
            instrumentObject(eval(item.object), item.instrumentedName, item.logSettings);
        });
    }
    // This whole function getInstrumentJS returns just the function `instrumentJS`.
    return instrumentJS;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoianMtaW5zdHJ1bWVudHMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi9zcmMvbGliL2pzLWluc3RydW1lbnRzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBLGlFQUFpRTtBQUNqRSxvRkFBb0Y7QUE4QnBGLE1BQU0sVUFBVSxlQUFlLENBQUMsT0FBZSxFQUFFLG9CQUFvQjtJQUNuRTs7O09BR0c7SUFFSCx3Q0FBd0M7SUFDeEMsb0NBQW9DO0lBQ3BDLHFDQUFxQztJQUNyQyxpQ0FBaUM7SUFDakMsaUNBQWlDO0lBQ2pDLHFCQUFxQjtJQUNyQixlQUFlO0lBQ2YsTUFBTTtJQUVOLG1FQUFtRTtJQUNuRSxNQUFNLFdBQVcsR0FBRyxHQUFHLENBQUM7SUFDeEIsYUFBYTtJQUNiLE1BQU0sVUFBVSxHQUFHLElBQUksTUFBTSxFQUFFLENBQUM7SUFDaEMsK0NBQStDO0lBQy9DLElBQUksS0FBSyxHQUFHLEtBQUssQ0FBQztJQUNsQixnREFBZ0Q7SUFDaEQsSUFBSSxPQUFPLEdBQUcsQ0FBQyxDQUFDO0lBRWhCLDBCQUEwQjtJQUMxQixNQUFNLFdBQVcsR0FBRztRQUNsQixJQUFJLEVBQUUsTUFBTTtRQUNaLEdBQUcsRUFBRSxLQUFLO1FBQ1YsVUFBVSxFQUFFLGFBQWE7UUFDekIsWUFBWSxFQUFFLGVBQWU7UUFDN0IsR0FBRyxFQUFFLEtBQUs7UUFDVixVQUFVLEVBQUUsYUFBYTtRQUN6QixhQUFhLEVBQUUsZ0JBQWdCO0tBQ2hDLENBQUM7SUFFRixvRkFBb0Y7SUFDcEYseUVBQXlFO0lBQ3pFLE1BQU0sQ0FBQyxxQkFBcUIsR0FBRyxVQUFVLE9BQU8sRUFBRSxJQUFJO1FBQ3BELElBQUksT0FBTyxLQUFLLFNBQVMsRUFBRTtZQUN6QixNQUFNLElBQUksS0FBSyxDQUFDLDZDQUE2QyxDQUFDLENBQUM7U0FDaEU7UUFDRCxJQUFJLEVBQUUsR0FBRyxNQUFNLENBQUMsd0JBQXdCLENBQUMsT0FBTyxFQUFFLElBQUksQ0FBQyxDQUFD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

/***/ }),

/***/ "../webext-instrumentation/build/module/lib/pending-navigation.js":
/*!************************************************************************!*\
  !*** ../webext-instrumentation/build/module/lib/pending-navigation.js ***!
  \************************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "PendingNavigation": () => (/* binding */ PendingNavigation)
/* harmony export */ });
/**
 * Ties together the two separate navigation events that together holds information about both parent frame id and transition-related attributes
 */
class PendingNavigation {
    onBeforeNavigateEventNavigation;
    onCommittedEventNavigation;
    resolveOnBeforeNavigateEventNavigation;
    resolveOnCommittedEventNavigation;
    constructor() {
        this.onBeforeNavigateEventNavigation = new Promise((resolve) => {
            this.resolveOnBeforeNavigateEventNavigation = resolve;
        });
        this.onCommittedEventNavigation = new Promise((resolve) => {
            this.resolveOnCommittedEventNavigation = resolve;
        });
    }
    resolved() {
        return Promise.all([
            this.onBeforeNavigateEventNavigation,
            this.onCommittedEventNavigation,
        ]);
    }
    /**
     * Either returns or times out and returns undefined or
     * returns the results from resolved() above
     *
     * @param ms
     */
    async resolvedWithinTimeout(ms) {
        const resolved = await Promise.race([
            this.resolved(),
            new Promise((resolve) => setTimeout(resolve, ms)),
        ]);
        return resolved;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGVuZGluZy1uYXZpZ2F0aW9uLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vc3JjL2xpYi9wZW5kaW5nLW5hdmlnYXRpb24udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBRUE7O0dBRUc7QUFDSCxNQUFNLE9BQU8saUJBQWlCO0lBQ1osK0JBQStCLENBQXNCO0lBQ3JELDBCQUEwQixDQUFzQjtJQUN6RCxzQ0FBc0MsQ0FBZ0M7SUFDdEUsaUNBQWlDLENBQWdDO0lBQ3hFO1FBQ0UsSUFBSSxDQUFDLCtCQUErQixHQUFHLElBQUksT0FBTyxDQUFDLENBQUMsT0FBTyxFQUFFLEVBQUU7WUFDN0QsSUFBSSxDQUFDLHNDQUFzQyxHQUFHLE9BQU8sQ0FBQztRQUN4RCxDQUFDLENBQUMsQ0FBQztRQUNILElBQUksQ0FBQywwQkFBMEIsR0FBRyxJQUFJLE9BQU8sQ0FBQyxDQUFDLE9BQU8sRUFBRSxFQUFFO1lBQ3hELElBQUksQ0FBQyxpQ0FBaUMsR0FBRyxPQUFPLENBQUM7UUFDbkQsQ0FBQyxDQUFDLENBQUM7SUFDTCxDQUFDO0lBQ00sUUFBUTtRQUNiLE9BQU8sT0FBTyxDQUFDLEdBQUcsQ0FBQztZQUNqQixJQUFJLENBQUMsK0JBQStCO1lBQ3BDLElBQUksQ0FBQywwQkFBMEI7U0FDaEMsQ0FBQyxDQUFDO0lBQ0wsQ0FBQztJQUVEOzs7OztPQUtHO0lBQ0ksS0FBSyxDQUFDLHFCQUFxQixDQUFDLEVBQUU7UUFDbkMsTUFBTSxRQUFRLEdBQUcsTUFBTSxPQUFPLENBQUMsSUFBSSxDQUFDO1lBQ2xDLElBQUksQ0FBQyxRQUFRLEVBQUU7WUFDZixJQUFJLE9BQU8sQ0FBQyxDQUFDLE9BQU8sRUFBRSxFQUFFLENBQUMsVUFBVSxDQUFDLE9BQU8sRUFBRSxFQUFFLENBQUMsQ0FBQztTQUNsRCxDQUFDLENBQUM7UUFDSCxPQUFPLFFBQVEsQ0FBQztJQUNsQixDQUFDO0NBQ0YifQ==

/***/ }),

/***/ "../webext-instrumentation/build/module/lib/pending-request.js":
/*!*********************************************************************!*\
  !*** ../webext-instrumentation/build/module/lib/pending-request.js ***!
  \*********************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "PendingRequest": () => (/* binding */ PendingRequest)
/* harmony export */ });
/**
 * Ties together the two separate events that together holds information about both request headers and body
 */
class PendingRequest {
    onBeforeRequestEventDetails;
    onBeforeSendHeadersEventDetails;
    resolveOnBeforeRequestEventDetails;
    resolveOnBeforeSendHeadersEventDetails;
    constructor() {
        this.onBeforeRequestEventDetails = new Promise((resolve) => {
            this.resolveOnBeforeRequestEventDetails = resolve;
        });
        this.onBeforeSendHeadersEventDetails = new Promise((resolve) => {
            this.resolveOnBeforeSendHeadersEventDetails = resolve;
        });
    }
    resolved() {
        return Promise.all([
            this.onBeforeRequestEventDetails,
            this.onBeforeSendHeadersEventDetails,
        ]);
    }
    /**
     * Either returns or times out and returns undefined or
     * returns the results from resolved() above
     *
     * @param ms
     */
    async resolvedWithinTimeout(ms) {
        const resolved = await Promise.race([
            this.resolved(),
            new Promise((resolve) => setTimeout(resolve, ms)),
        ]);
        return resolved;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGVuZGluZy1yZXF1ZXN0LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vc3JjL2xpYi9wZW5kaW5nLXJlcXVlc3QudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBS0E7O0dBRUc7QUFDSCxNQUFNLE9BQU8sY0FBYztJQUNULDJCQUEyQixDQUFpRDtJQUM1RSwrQkFBK0IsQ0FBcUQ7SUFDN0Ysa0NBQWtDLENBRS9CO0lBQ0gsc0NBQXNDLENBRW5DO0lBQ1Y7UUFDRSxJQUFJLENBQUMsMkJBQTJCLEdBQUcsSUFBSSxPQUFPLENBQUMsQ0FBQyxPQUFPLEVBQUUsRUFBRTtZQUN6RCxJQUFJLENBQUMsa0NBQWtDLEdBQUcsT0FBTyxDQUFDO1FBQ3BELENBQUMsQ0FBQyxDQUFDO1FBQ0gsSUFBSSxDQUFDLCtCQUErQixHQUFHLElBQUksT0FBTyxDQUFDLENBQUMsT0FBTyxFQUFFLEVBQUU7WUFDN0QsSUFBSSxDQUFDLHNDQUFzQyxHQUFHLE9BQU8sQ0FBQztRQUN4RCxDQUFDLENBQUMsQ0FBQztJQUNMLENBQUM7SUFDTSxRQUFRO1FBQ2IsT0FBTyxPQUFPLENBQUMsR0FBRyxDQUFDO1lBQ2pCLElBQUksQ0FBQywyQkFBMkI7WUFDaEMsSUFBSSxDQUFDLCtCQUErQjtTQUNyQyxDQUFDLENBQUM7SUFDTCxDQUFDO0lBRUQ7Ozs7O09BS0c7SUFDSSxLQUFLLENBQUMscUJBQXFCLENBQUMsRUFBRTtRQUNuQyxNQUFNLFFBQVEsR0FBRyxNQUFNLE9BQU8sQ0FBQyxJQUFJLENBQUM7WUFDbEMsSUFBSSxDQUFDLFFBQVEsRUFBRTtZQUNmLElBQUksT0FBTyxDQUFDLENBQUMsT0FBTyxFQUFFLEVBQUUsQ0FBQyxVQUFVLENBQUMsT0FBTyxFQUFFLEVBQUUsQ0FBQyxDQUFDO1NBQ2xELENBQUMsQ0FBQztRQUNILE9BQU8sUUFBUSxDQUFDO0lBQ2xCLENBQUM7Q0FDRiJ9

/***/ }),

/***/ "../webext-instrumentation/build/module/lib/pending-response.js":
/*!**********************************************************************!*\
  !*** ../webext-instrumentation/build/module/lib/pending-response.js ***!
  \**********************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "PendingResponse": () => (/* binding */ PendingResponse)
/* harmony export */ });
/* harmony import */ var _response_body_listener__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ./response-body-listener */ "../webext-instrumentation/build/module/lib/response-body-listener.js");

/**
 * Ties together the two separate events that together holds information about both response headers and body
 */
class PendingResponse {
    onBeforeRequestEventDetails;
    onCompletedEventDetails;
    responseBodyListener;
    resolveOnBeforeRequestEventDetails;
    resolveOnCompletedEventDetails;
    constructor() {
        this.onBeforeRequestEventDetails = new Promise((resolve) => {
            this.resolveOnBeforeRequestEventDetails = resolve;
        });
        this.onCompletedEventDetails = new Promise((resolve) => {
            this.resolveOnCompletedEventDetails = resolve;
        });
    }
    addResponseResponseBodyListener(details) {
        this.responseBodyListener = new _response_body_listener__WEBPACK_IMPORTED_MODULE_0__.ResponseBodyListener(details);
    }
    resolved() {
        return Promise.all([
            this.onBeforeRequestEventDetails,
            this.onCompletedEventDetails,
        ]);
    }
    /**
     * Either returns or times out and returns undefined or
     * returns the results from resolved() above
     *
     * @param ms
     */
    async resolvedWithinTimeout(ms) {
        const resolved = await Promise.race([
            this.resolved(),
            new Promise((resolve) => setTimeout(resolve, ms)),
        ]);
        return resolved;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGVuZGluZy1yZXNwb25zZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uL3NyYy9saWIvcGVuZGluZy1yZXNwb25zZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFJQSxPQUFPLEVBQUUsb0JBQW9CLEVBQUUsTUFBTSwwQkFBMEIsQ0FBQztBQUVoRTs7R0FFRztBQUNILE1BQU0sT0FBTyxlQUFlO0lBQ1YsMkJBQTJCLENBQWlEO0lBQzVFLHVCQUF1QixDQUE2QztJQUM3RSxvQkFBb0IsQ0FBdUI7SUFDM0Msa0NBQWtDLENBRS9CO0lBQ0gsOEJBQThCLENBRTNCO0lBQ1Y7UUFDRSxJQUFJLENBQUMsMkJBQTJCLEdBQUcsSUFBSSxPQUFPLENBQUMsQ0FBQyxPQUFPLEVBQUUsRUFBRTtZQUN6RCxJQUFJLENBQUMsa0NBQWtDLEdBQUcsT0FBTyxDQUFDO1FBQ3BELENBQUMsQ0FBQyxDQUFDO1FBQ0gsSUFBSSxDQUFDLHVCQUF1QixHQUFHLElBQUksT0FBTyxDQUFDLENBQUMsT0FBTyxFQUFFLEVBQUU7WUFDckQsSUFBSSxDQUFDLDhCQUE4QixHQUFHLE9BQU8sQ0FBQztRQUNoRCxDQUFDLENBQUMsQ0FBQztJQUNMLENBQUM7SUFDTSwrQkFBK0IsQ0FDcEMsT0FBOEM7UUFFOUMsSUFBSSxDQUFDLG9CQUFvQixHQUFHLElBQUksb0JBQW9CLENBQUMsT0FBTyxDQUFDLENBQUM7SUFDaEUsQ0FBQztJQUNNLFFBQVE7UUFDYixPQUFPLE9BQU8sQ0FBQyxHQUFHLENBQUM7WUFDakIsSUFBSSxDQUFDLDJCQUEyQjtZQUNoQyxJQUFJLENBQUMsdUJBQXVCO1NBQzdCLENBQUMsQ0FBQztJQUNMLENBQUM7SUFFRDs7Ozs7T0FLRztJQUNJLEtBQUssQ0FBQyxxQkFBcUIsQ0FBQyxFQUFFO1FBQ25DLE1BQU0sUUFBUSxHQUFHLE1BQU0sT0FBTyxDQUFDLElBQUksQ0FBQztZQUNsQyxJQUFJLENBQUMsUUFBUSxFQUFFO1lBQ2YsSUFBSSxPQUFPLENBQUMsQ0FBQyxPQUFPLEVBQUUsRUFBRSxDQUFDLFVBQVUsQ0FBQyxPQUFPLEVBQUUsRUFBRSxDQUFDLENBQUM7U0FDbEQsQ0FBQyxDQUFDO1FBQ0gsT0FBTyxRQUFRLENBQUM7SUFDbEIsQ0FBQztDQUNGIn0=

/***/ }),

/***/ "../webext-instrumentation/build/module/lib/response-body-listener.js":
/*!****************************************************************************!*\
  !*** ../webext-instrumentation/build/module/lib/response-body-listener.js ***!
  \****************************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ResponseBodyListener": () => (/* binding */ ResponseBodyListener)
/* harmony export */ });
/* harmony import */ var _sha256__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ./sha256 */ "../webext-instrumentation/build/module/lib/sha256.js");

class ResponseBodyListener {
    responseBody;
    contentHash;
    resolveResponseBody;
    resolveContentHash;
    constructor(details) {
        this.responseBody = new Promise((resolve) => {
            this.resolveResponseBody = resolve;
        });
        this.contentHash = new Promise((resolve) => {
            this.resolveContentHash = resolve;
        });
        // Used to parse Response stream
        const filter = browser.webRequest.filterResponseData(details.requestId.toString());
        let responseBody = new Uint8Array();
        filter.ondata = (event) => {
            (0,_sha256__WEBPACK_IMPORTED_MODULE_0__.digestMessage)(event.data).then((digest) => {
                this.resolveContentHash(digest);
            });
            const incoming = new Uint8Array(event.data);
            const tmp = new Uint8Array(responseBody.length + incoming.length);
            tmp.set(responseBody);
            tmp.set(incoming, responseBody.length);
            responseBody = tmp;
            filter.write(event.data);
        };
        filter.onstop = (_event) => {
            this.resolveResponseBody(responseBody);
            filter.disconnect();
        };
    }
    async getResponseBody() {
        return this.responseBody;
    }
    async getContentHash() {
        return this.contentHash;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVzcG9uc2UtYm9keS1saXN0ZW5lci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uL3NyYy9saWIvcmVzcG9uc2UtYm9keS1saXN0ZW5lci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFDQSxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sVUFBVSxDQUFDO0FBRXpDLE1BQU0sT0FBTyxvQkFBb0I7SUFDZCxZQUFZLENBQXNCO0lBQ2xDLFdBQVcsQ0FBa0I7SUFDdEMsbUJBQW1CLENBQXFDO0lBQ3hELGtCQUFrQixDQUFnQztJQUUxRCxZQUFZLE9BQThDO1FBQ3hELElBQUksQ0FBQyxZQUFZLEdBQUcsSUFBSSxPQUFPLENBQUMsQ0FBQyxPQUFPLEVBQUUsRUFBRTtZQUMxQyxJQUFJLENBQUMsbUJBQW1CLEdBQUcsT0FBTyxDQUFDO1FBQ3JDLENBQUMsQ0FBQyxDQUFDO1FBQ0gsSUFBSSxDQUFDLFdBQVcsR0FBRyxJQUFJLE9BQU8sQ0FBQyxDQUFDLE9BQU8sRUFBRSxFQUFFO1lBQ3pDLElBQUksQ0FBQyxrQkFBa0IsR0FBRyxPQUFPLENBQUM7UUFDcEMsQ0FBQyxDQUFDLENBQUM7UUFFSCxnQ0FBZ0M7UUFDaEMsTUFBTSxNQUFNLEdBQVEsT0FBTyxDQUFDLFVBQVUsQ0FBQyxrQkFBa0IsQ0FDdkQsT0FBTyxDQUFDLFNBQVMsQ0FBQyxRQUFRLEVBQUUsQ0FDdEIsQ0FBQztRQUVULElBQUksWUFBWSxHQUFHLElBQUksVUFBVSxFQUFFLENBQUM7UUFDcEMsTUFBTSxDQUFDLE1BQU0sR0FBRyxDQUFDLEtBQUssRUFBRSxFQUFFO1lBQ3hCLGFBQWEsQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUFDLENBQUMsSUFBSSxDQUFDLENBQUMsTUFBTSxFQUFFLEVBQUU7Z0JBQ3hDLElBQUksQ0FBQyxrQkFBa0IsQ0FBQyxNQUFNLENBQUMsQ0FBQztZQUNsQyxDQUFDLENBQUMsQ0FBQztZQUNILE1BQU0sUUFBUSxHQUFHLElBQUksVUFBVSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUM1QyxNQUFNLEdBQUcsR0FBRyxJQUFJLFVBQVUsQ0FBQyxZQUFZLENBQUMsTUFBTSxHQUFHLFFBQVEsQ0FBQyxNQUFNLENBQUMsQ0FBQztZQUNsRSxHQUFHLENBQUMsR0FBRyxDQUFDLFlBQVksQ0FBQyxDQUFDO1lBQ3RCLEdBQUcsQ0FBQyxHQUFHLENBQUMsUUFBUSxFQUFFLFlBQVksQ0FBQyxNQUFNLENBQUMsQ0FBQztZQUN2QyxZQUFZLEdBQUcsR0FBRyxDQUFDO1lBQ25CLE1BQU0sQ0FBQyxLQUFLLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxDQUFDO1FBQzNCLENBQUMsQ0FBQztRQUVGLE1BQU0sQ0FBQyxNQUFNLEdBQUcsQ0FBQyxNQUFNLEVBQUUsRUFBRTtZQUN6QixJQUFJLENBQUMsbUJBQW1CLENBQUMsWUFBWSxDQUFDLENBQUM7WUFDdkMsTUFBTSxDQUFDLFVBQVUsRUFBRSxDQUFDO1FBQ3RCLENBQUMsQ0FBQztJQUNKLENBQUM7SUFFTSxLQUFLLENBQUMsZUFBZTtRQUMxQixPQUFPLElBQUksQ0FBQyxZQUFZLENBQUM7SUFDM0IsQ0FBQztJQUVNLEtBQUssQ0FBQyxjQUFjO1FBQ3pCLE9BQU8sSUFBSSxDQUFDLFdBQVcsQ0FBQztJQUMxQixDQUFDO0NBQ0YifQ==

/***/ }),

/***/ "../webext-instrumentation/build/module/lib/sha256.js":
/*!************************************************************!*\
  !*** ../webext-instrumentation/build/module/lib/sha256.js ***!
  \************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "digestMessage": () => (/* binding */ digestMessage)
/* harmony export */ });
/**
 * Code from the example at
 * https://developer.mozilla.org/en-US/docs/Web/API/SubtleCrypto/digest
 */
async function digestMessage(msgUint8) {
    const hashBuffer = await crypto.subtle.digest("SHA-256", msgUint8); // hash the message
    const hashArray = Array.from(new Uint8Array(hashBuffer)); // convert buffer to byte array
    const hashHex = hashArray
        .map((b) => b.toString(16).padStart(2, "0"))
        .join(""); // convert bytes to hex string
    return hashHex;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2hhMjU2LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vc3JjL2xpYi9zaGEyNTYudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7OztHQUdHO0FBRUgsTUFBTSxDQUFDLEtBQUssVUFBVSxhQUFhLENBQUMsUUFBb0I7SUFDdEQsTUFBTSxVQUFVLEdBQUcsTUFBTSxNQUFNLENBQUMsTUFBTSxDQUFDLE1BQU0sQ0FBQyxTQUFTLEVBQUUsUUFBUSxDQUFDLENBQUMsQ0FBQyxtQkFBbUI7SUFDdkYsTUFBTSxTQUFTLEdBQUcsS0FBSyxDQUFDLElBQUksQ0FBQyxJQUFJLFVBQVUsQ0FBQyxVQUFVLENBQUMsQ0FBQyxDQUFDLENBQUMsK0JBQStCO0lBQ3pGLE1BQU0sT0FBTyxHQUFHLFNBQVM7U0FDdEIsR0FBRyxDQUFDLENBQUMsQ0FBQyxFQUFFLEVBQUUsQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDLEVBQUUsQ0FBQyxDQUFDLFFBQVEsQ0FBQyxDQUFDLEVBQUUsR0FBRyxDQUFDLENBQUM7U0FDM0MsSUFBSSxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsOEJBQThCO0lBQzNDLE9BQU8sT0FBTyxDQUFDO0FBQ2pCLENBQUMifQ==

/***/ }),

/***/ "../webext-instrumentation/build/module/lib/string-utils.js":
/*!******************************************************************!*\
  !*** ../webext-instrumentation/build/module/lib/string-utils.js ***!
  \******************************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "encode_utf8": () => (/* binding */ encode_utf8),
/* harmony export */   "escapeString": () => (/* binding */ escapeString),
/* harmony export */   "escapeUrl": () => (/* binding */ escapeUrl),
/* harmony export */   "Uint8ToBase64": () => (/* binding */ Uint8ToBase64),
/* harmony export */   "boolToInt": () => (/* binding */ boolToInt)
/* harmony export */ });
function encode_utf8(s) {
    return unescape(encodeURIComponent(s));
}
const escapeString = function (str) {
    // Convert to string if necessary
    if (typeof str !== "string") {
        str = String(str);
    }
    return encode_utf8(str);
};
const escapeUrl = function (url, stripDataUrlData = true) {
    url = escapeString(url);
    // data:[<mediatype>][;base64],<data>
    if (url.substr(0, 5) === "data:" &&
        stripDataUrlData &&
        url.indexOf(",") > -1) {
        url = url.substr(0, url.indexOf(",") + 1) + "<data-stripped>";
    }
    return url;
};
// Base64 encoding, found on:
// https://stackoverflow.com/questions/12710001/how-to-convert-uint8-array-to-base64-encoded-string/25644409#25644409
const Uint8ToBase64 = function (u8Arr) {
    const CHUNK_SIZE = 0x8000; // arbitrary number
    let index = 0;
    const length = u8Arr.length;
    let result = "";
    let slice;
    while (index < length) {
        slice = u8Arr.subarray(index, Math.min(index + CHUNK_SIZE, length));
        result += String.fromCharCode.apply(null, slice);
        index += CHUNK_SIZE;
    }
    return btoa(result);
};
const boolToInt = function (bool) {
    return bool ? 1 : 0;
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3RyaW5nLXV0aWxzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vc3JjL2xpYi9zdHJpbmctdXRpbHMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUEsTUFBTSxVQUFVLFdBQVcsQ0FBQyxDQUFDO0lBQzNCLE9BQU8sUUFBUSxDQUFDLGtCQUFrQixDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7QUFDekMsQ0FBQztBQUVELE1BQU0sQ0FBQyxNQUFNLFlBQVksR0FBRyxVQUFVLEdBQVE7SUFDNUMsaUNBQWlDO0lBQ2pDLElBQUksT0FBTyxHQUFHLEtBQUssUUFBUSxFQUFFO1FBQzNCLEdBQUcsR0FBRyxNQUFNLENBQUMsR0FBRyxDQUFDLENBQUM7S0FDbkI7SUFFRCxPQUFPLFdBQVcsQ0FBQyxHQUFHLENBQUMsQ0FBQztBQUMxQixDQUFDLENBQUM7QUFFRixNQUFNLENBQUMsTUFBTSxTQUFTLEdBQUcsVUFDdkIsR0FBVyxFQUNYLG1CQUE0QixJQUFJO0lBRWhDLEdBQUcsR0FBRyxZQUFZLENBQUMsR0FBRyxDQUFDLENBQUM7SUFDeEIscUNBQXFDO0lBQ3JDLElBQ0UsR0FBRyxDQUFDLE1BQU0sQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLEtBQUssT0FBTztRQUM1QixnQkFBZ0I7UUFDaEIsR0FBRyxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsR0FBRyxDQUFDLENBQUMsRUFDckI7UUFDQSxHQUFHLEdBQUcsR0FBRyxDQUFDLE1BQU0sQ0FBQyxDQUFDLEVBQUUsR0FBRyxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsR0FBRyxDQUFDLENBQUMsR0FBRyxpQkFBaUIsQ0FBQztLQUMvRDtJQUNELE9BQU8sR0FBRyxDQUFDO0FBQ2IsQ0FBQyxDQUFDO0FBRUYsNkJBQTZCO0FBQzdCLHFIQUFxSDtBQUNySCxNQUFNLENBQUMsTUFBTSxhQUFhLEdBQUcsVUFBVSxLQUFpQjtJQUN0RCxNQUFNLFVBQVUsR0FBRyxNQUFNLENBQUMsQ0FBQyxtQkFBbUI7SUFDOUMsSUFBSSxLQUFLLEdBQUcsQ0FBQyxDQUFDO0lBQ2QsTUFBTSxNQUFNLEdBQUcsS0FBSyxDQUFDLE1BQU0sQ0FBQztJQUM1QixJQUFJLE1BQU0sR0FBRyxFQUFFLENBQUM7SUFDaEIsSUFBSSxLQUFpQixDQUFDO0lBQ3RCLE9BQU8sS0FBSyxHQUFHLE1BQU0sRUFBRTtRQUNyQixLQUFLLEdBQUcsS0FBSyxDQUFDLFFBQVEsQ0FBQyxLQUFLLEVBQUUsSUFBSSxDQUFDLEdBQUcsQ0FBQyxLQUFLLEdBQUcsVUFBVSxFQUFFLE1BQU0sQ0FBQyxDQUFDLENBQUM7UUFDcEUsTUFBTSxJQUFJLE1BQU0sQ0FBQyxZQUFZLENBQUMsS0FBSyxDQUFDLElBQUksRUFBRSxLQUFLLENBQUMsQ0FBQztRQUNqRCxLQUFLLElBQUksVUFBVSxDQUFDO0tBQ3JCO0lBQ0QsT0FBTyxJQUFJLENBQUMsTUFBTSxDQUFDLENBQUM7QUFDdEIsQ0FBQyxDQUFDO0FBRUYsTUFBTSxDQUFDLE1BQU0sU0FBUyxHQUFHLFVBQVUsSUFBYTtJQUM5QyxPQUFPLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7QUFDdEIsQ0FBQyxDQUFDIn0=

/***/ }),

/***/ "../webext-instrumentation/build/module/lib/uuid.js":
/*!**********************************************************!*\
  !*** ../webext-instrumentation/build/module/lib/uuid.js ***!
  \**********************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "makeUUID": () => (/* binding */ makeUUID)
/* harmony export */ });
/* eslint-disable no-bitwise */
// from https://gist.github.com/jed/982883#gistcomment-2403369
const hex = [];
for (let i = 0; i < 256; i++) {
    hex[i] = (i < 16 ? "0" : "") + i.toString(16);
}
const makeUUID = () => {
    const r = crypto.getRandomValues(new Uint8Array(16));
    r[6] = (r[6] & 0x0f) | 0x40;
    r[8] = (r[8] & 0x3f) | 0x80;
    return (hex[r[0]] +
        hex[r[1]] +
        hex[r[2]] +
        hex[r[3]] +
        "-" +
        hex[r[4]] +
        hex[r[5]] +
        "-" +
        hex[r[6]] +
        hex[r[7]] +
        "-" +
        hex[r[8]] +
        hex[r[9]] +
        "-" +
        hex[r[10]] +
        hex[r[11]] +
        hex[r[12]] +
        hex[r[13]] +
        hex[r[14]] +
        hex[r[15]]);
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXVpZC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uL3NyYy9saWIvdXVpZC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQSwrQkFBK0I7QUFFL0IsOERBQThEO0FBQzlELE1BQU0sR0FBRyxHQUFHLEVBQUUsQ0FBQztBQUVmLEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxHQUFHLEVBQUUsQ0FBQyxFQUFFLEVBQUU7SUFDNUIsR0FBRyxDQUFDLENBQUMsQ0FBQyxHQUFHLENBQUMsQ0FBQyxHQUFHLEVBQUUsQ0FBQyxDQUFDLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUMsR0FBRyxDQUFDLENBQUMsUUFBUSxDQUFDLEVBQUUsQ0FBQyxDQUFDO0NBQy9DO0FBRUQsTUFBTSxDQUFDLE1BQU0sUUFBUSxHQUFHLEdBQUcsRUFBRTtJQUMzQixNQUFNLENBQUMsR0FBRyxNQUFNLENBQUMsZUFBZSxDQUFDLElBQUksVUFBVSxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUM7SUFFckQsQ0FBQyxDQUFDLENBQUMsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxHQUFHLElBQUksQ0FBQyxHQUFHLElBQUksQ0FBQztJQUM1QixDQUFDLENBQUMsQ0FBQyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLEdBQUcsSUFBSSxDQUFDLEdBQUcsSUFBSSxDQUFDO0lBRTVCLE9BQU8sQ0FDTCxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ1QsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUNULEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDVCxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ1QsR0FBRztRQUNILEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDVCxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ1QsR0FBRztRQUNILEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDVCxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ1QsR0FBRztRQUNILEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDVCxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ1QsR0FBRztRQUNILEdBQUcsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUM7UUFDVixHQUFHLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDO1FBQ1YsR0FBRyxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQztRQUNWLEdBQUcsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUM7UUFDVixHQUFHLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDO1FBQ1YsR0FBRyxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQyxDQUNYLENBQUM7QUFDSixDQUFDLENBQUMifQ==

/***/ }),

/***/ "../webext-instrumentation/build/module/schema.js":
/*!********************************************************!*\
  !*** ../webext-instrumentation/build/module/schema.js ***!
  \********************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "dateTimeUnicodeFormatString": () => (/* binding */ dateTimeUnicodeFormatString)
/* harmony export */ });
// https://www.unicode.org/reports/tr35/tr35-dates.html#Date_Field_Symbol_Table
const dateTimeUnicodeFormatString = "yyyy-MM-dd'T'HH:mm:ss.SSSXX";
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2NoZW1hLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vc3JjL3NjaGVtYS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFJQSwrRUFBK0U7QUFDL0UsTUFBTSxDQUFDLE1BQU0sMkJBQTJCLEdBQUcsNkJBQTZCLENBQUMifQ==

/***/ })

/******/ 	});
/************************************************************************/
/******/ 	// The module cache
/******/ 	var __webpack_module_cache__ = {};
/******/ 	
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/ 		// Check if module is in cache
/******/ 		var cachedModule = __webpack_module_cache__[moduleId];
/******/ 		if (cachedModule !== undefined) {
/******/ 			return cachedModule.exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = __webpack_module_cache__[moduleId] = {
/******/ 			// no module.id needed
/******/ 			// no module.loaded needed
/******/ 			exports: {}
/******/ 		};
/******/ 	
/******/ 		// Execute the module function
/******/ 		__webpack_modules__[moduleId](module, module.exports, __webpack_require__);
/******/ 	
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/ 	
/************************************************************************/
/******/ 	/* webpack/runtime/define property getters */
/******/ 	(() => {
/******/ 		// define getter functions for harmony exports
/******/ 		__webpack_require__.d = (exports, definition) => {
/******/ 			for(var key in definition) {
/******/ 				if(__webpack_require__.o(definition, key) && !__webpack_require__.o(exports, key)) {
/******/ 					Object.defineProperty(exports, key, { enumerable: true, get: definition[key] });
/******/ 				}
/******/ 			}
/******/ 		};
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/hasOwnProperty shorthand */
/******/ 	(() => {
/******/ 		__webpack_require__.o = (obj, prop) => (Object.prototype.hasOwnProperty.call(obj, prop))
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/make namespace object */
/******/ 	(() => {
/******/ 		// define __esModule on exports
/******/ 		__webpack_require__.r = (exports) => {
/******/ 			if(typeof Symbol !== 'undefined' && Symbol.toStringTag) {
/******/ 				Object.defineProperty(exports, Symbol.toStringTag, { value: 'Module' });
/******/ 			}
/******/ 			Object.defineProperty(exports, '__esModule', { value: true });
/******/ 		};
/******/ 	})();
/******/ 	
/************************************************************************/
var __webpack_exports__ = {};
// This entry need to be wrapped in an IIFE because it need to be isolated against other modules in the chunk.
(() => {
/*!*****************************!*\
  !*** ./feature.js/index.js ***!
  \*****************************/
__webpack_require__.r(__webpack_exports__);
/* harmony import */ var openwpm_webext_instrumentation__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! openwpm-webext-instrumentation */ "../webext-instrumentation/build/module/index.js");
/* harmony import */ var _loggingdb_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ./loggingdb.js */ "./feature.js/loggingdb.js");
/* harmony import */ var _callstack_instrument_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! ./callstack-instrument.js */ "./feature.js/callstack-instrument.js");





async function main() {
  // Read the browser configuration from file
  let filename = "browser_params.json";
  let config = await browser.profileDirIO.readFile(filename);
  if (config) {
    config = JSON.parse(config);
    console.log("Browser Config:", config);
  } else {
    config = {
      navigation_instrument:true,
      cookie_instrument:true,
      js_instrument:true,
      cleaned_js_instrument_settings:
      [
        {
          object: `window.CanvasRenderingContext2D.prototype`,
          instrumentedName: "CanvasRenderingContext2D",
          logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
          }
        },
      ],
      http_instrument:true,
      callstack_instrument:true,
      save_content:false,
      testing:true,
      browser_id:0,
      custom_params: {}
    };
    console.log("WARNING: config not found. Assuming this is a test run of",
                "the extension. Outputting all queries to console.", {config});
  }

  await _loggingdb_js__WEBPACK_IMPORTED_MODULE_1__.open(config['storage_controller_address'],
                       config['logger_address'],
                       config['browser_id']);

  if (config["custom_params"]["pre_instrumentation_code"]) {
    eval(config["custom_params"]["pre_instrumentation_code"])
  }
  if (config["navigation_instrument"]) {
    _loggingdb_js__WEBPACK_IMPORTED_MODULE_1__.logDebug("Navigation instrumentation enabled");
    let navigationInstrument = new openwpm_webext_instrumentation__WEBPACK_IMPORTED_MODULE_0__.NavigationInstrument(_loggingdb_js__WEBPACK_IMPORTED_MODULE_1__);
    navigationInstrument.run(config["browser_id"]);
  }

  if (config['cookie_instrument']) {
    _loggingdb_js__WEBPACK_IMPORTED_MODULE_1__.logDebug("Cookie instrumentation enabled");
    let cookieInstrument = new openwpm_webext_instrumentation__WEBPACK_IMPORTED_MODULE_0__.CookieInstrument(_loggingdb_js__WEBPACK_IMPORTED_MODULE_1__);
    cookieInstrument.run(config['browser_id']);
  }

  if (config['js_instrument']) {
    _loggingdb_js__WEBPACK_IMPORTED_MODULE_1__.logDebug("Javascript instrumentation enabled");
    let jsInstrument = new openwpm_webext_instrumentation__WEBPACK_IMPORTED_MODULE_0__.JavascriptInstrument(_loggingdb_js__WEBPACK_IMPORTED_MODULE_1__);
    jsInstrument.run(config['browser_id']);
    await jsInstrument.registerContentScript(config['testing'], config['cleaned_js_instrument_settings']);
  }

  if (config['http_instrument']) {
    _loggingdb_js__WEBPACK_IMPORTED_MODULE_1__.logDebug("HTTP Instrumentation enabled");
    let httpInstrument = new openwpm_webext_instrumentation__WEBPACK_IMPORTED_MODULE_0__.HttpInstrument(_loggingdb_js__WEBPACK_IMPORTED_MODULE_1__);
    httpInstrument.run(config['browser_id'],
                       config['save_content']);
  }

  if (config['callstack_instrument']) {
    _loggingdb_js__WEBPACK_IMPORTED_MODULE_1__.logDebug("Callstack Instrumentation enabled");
    let callstackInstrument = new _callstack_instrument_js__WEBPACK_IMPORTED_MODULE_2__.CallstackInstrument(_loggingdb_js__WEBPACK_IMPORTED_MODULE_1__);
    callstackInstrument.run(config['browser_id']);
  }
  
  if (config['dns_instrument']) {
    _loggingdb_js__WEBPACK_IMPORTED_MODULE_1__.logDebug("DNS instrumentation enabled");
    let dnsInstrument = new openwpm_webext_instrumentation__WEBPACK_IMPORTED_MODULE_0__.DnsInstrument(_loggingdb_js__WEBPACK_IMPORTED_MODULE_1__);
    dnsInstrument.run(config['browser_id']);
  }

  await browser.profileDirIO.writeFile("OPENWPM_STARTUP_SUCCESS.txt", "");
}

main();


})();

/******/ })()
;
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uL2ZlYXR1cmUuanMvY2FsbHN0YWNrLWluc3RydW1lbnQuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi9mZWF0dXJlLmpzL2xvZ2dpbmdkYi5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uL2ZlYXR1cmUuanMvc29ja2V0LmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2JhY2tncm91bmQvY29va2llLWluc3RydW1lbnQuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvYmFja2dyb3VuZC9kbnMtaW5zdHJ1bWVudC5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uLi93ZWJleHQtaW5zdHJ1bWVudGF0aW9uL2J1aWxkL21vZHVsZS9iYWNrZ3JvdW5kL2h0dHAtaW5zdHJ1bWVudC5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uLi93ZWJleHQtaW5zdHJ1bWVudGF0aW9uL2J1aWxkL21vZHVsZS9iYWNrZ3JvdW5kL2phdmFzY3JpcHQtaW5zdHJ1bWVudC5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uLi93ZWJleHQtaW5zdHJ1bWVudGF0aW9uL2J1aWxkL21vZHVsZS9iYWNrZ3JvdW5kL25hdmlnYXRpb24taW5zdHJ1bWVudC5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uLi93ZWJleHQtaW5zdHJ1bWVudGF0aW9uL2J1aWxkL21vZHVsZS9jb250ZW50L2phdmFzY3JpcHQtaW5zdHJ1bWVudC1jb250ZW50LXNjb3BlLmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2NvbnRlbnQvamF2YXNjcmlwdC1pbnN0cnVtZW50LXBhZ2Utc2NvcGUuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvaW5kZXguanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvbGliL2V4dGVuc2lvbi1zZXNzaW9uLWV2ZW50LW9yZGluYWwuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvbGliL2V4dGVuc2lvbi1zZXNzaW9uLXV1aWQuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvbGliL2h0dHAtcG9zdC1wYXJzZXIuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvbGliL2pzLWluc3RydW1lbnRzLmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2xpYi9wZW5kaW5nLW5hdmlnYXRpb24uanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvbGliL3BlbmRpbmctcmVxdWVzdC5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uLi93ZWJleHQtaW5zdHJ1bWVudGF0aW9uL2J1aWxkL21vZHVsZS9saWIvcGVuZGluZy1yZXNwb25zZS5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uLi93ZWJleHQtaW5zdHJ1bWVudGF0aW9uL2J1aWxkL21vZHVsZS9saWIvcmVzcG9uc2UtYm9keS1saXN0ZW5lci5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uLi93ZWJleHQtaW5zdHJ1bWVudGF0aW9uL2J1aWxkL21vZHVsZS9saWIvc2hhMjU2LmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2xpYi9zdHJpbmctdXRpbHMuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvbGliL3V1aWQuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvc2NoZW1hLmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94L3dlYnBhY2svYm9vdHN0cmFwIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94L3dlYnBhY2svcnVudGltZS9kZWZpbmUgcHJvcGVydHkgZ2V0dGVycyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC93ZWJwYWNrL3J1bnRpbWUvaGFzT3duUHJvcGVydHkgc2hvcnRoYW5kIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94L3dlYnBhY2svcnVudGltZS9tYWtlIG5hbWVzcGFjZSBvYmplY3QiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi9mZWF0dXJlLmpzL2luZGV4LmpzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7O0FBQUE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNPO0FBQ1A7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFD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QTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsdUJBQXVCLHFCQUFxQjtBQUM1QztBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsYUFBYTtBQUNiO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLDRDQUE0QywyQkFBMkIsY0FBYyxPQUFPLGdCQUFnQjtBQUM1RztBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxnREFBZ0QsMkJBQTJCLGNBQWMsT0FBTyxnQkFBZ0I7QUFDaEg7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxtQ0FBbUM7QUFDbkM7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsK0RBQStEO0FBQy9EO0FBQ0EsMkNBQTJDO0FBQzNDO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7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QUFzRSxPQUFPO0FBQzdFOztBQUVBLFFBQVEsK0NBQWM7QUFDdEI7QUFDQTs7QUFFQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLElBQUksbURBQWtCO0FBQ3RCLG1DQUFtQyxnRkFBb0IsQ0FBQywwQ0FBUztBQUNqRTtBQUNBOztBQUVBO0FBQ0EsSUFBSSxtREFBa0I7QUFDdEIsK0JBQStCLDRFQUFnQixDQUFDLDBDQUFTO0FBQ3pEO0FBQ0E7O0FBRUE7QUFDQSxJQUFJLG1EQUFrQjtBQUN0QiwyQkFBMkIsZ0ZBQW9CLENBQUMsMENBQVM7QUFDekQ7QUFDQTtBQUNBOztBQUVBO0FBQ0EsSUFBSSxtREFBa0I7QUFDdEIsNkJBQTZCLDBFQUFjLENBQUMsMENBQVM7QUFDckQ7QUFDQTtBQUNBOztBQUVBO0FBQ0EsSUFBSSxtREFBa0I7QUFDdEIsa0NBQWtDLHlFQUFtQixDQUFDLDBDQUFTO0FBQy9EO0FBQ0E7O0FBRUE7QUFDQSxJQUFJLG1EQUFrQjtBQUN0Qiw0QkFBNEIseUVBQWEsQ0FBQywwQ0FBUztBQUNuRDtBQUNBOztBQUVBO0FBQ0E7O0FBRUEiLCJmaWxlIjoiZmVhdHVyZS5qcyIsInNvdXJjZXNDb250ZW50IjpbIi8qXG4gIFdlIGNhcHR1cmUgdGhlIEpTIGNhbGxzdGFjayB3aGVuIHdlIGRldGVjdCBhIGR5bmFtaWNhbGx5IGNyZWF0ZWQgaHR0cCByZXF1ZXN0XG4gIGFuZCBidWJibGUgaXQgdXAgdmlhIGEgV2ViRXh0ZW5zaW9uIEV4cGVyaW1lbnQgQVBJIHN0YWNrRHVtcC5cbiAgVGhpcyBpbnN0cnVtZW50YXRpb24gY2FwdHVyZXMgdGhvc2UgYW5kIHNhdmVzIHRoZW0gdG8gdGhlIFwiY2FsbHN0YWNrc1wiIHRhYmxlLlxuKi9cbmV4cG9ydCBjbGFzcyBDYWxsc3RhY2tJbnN0cnVtZW50IHtcbiAgY29uc3RydWN0b3IoZGF0YVJlY2VpdmVyKSB7XG4gICAgdGhpcy5kYXRhUmVjZWl2ZXIgPSBkYXRhUmVjZWl2ZXI7XG4gIH1cbiAgcnVuKGJyb3dzZXJfaWQpIHtcbiAgICBicm93c2VyLnN0YWNrRHVtcC5vblN0YWNrQXZhaWxhYmxlLmFkZExpc3RlbmVyKChyZXF1ZXN0X2lkLCBjYWxsX3N0YWNrKSA9PiB7XG4gICAgICBjb25zdCByZWNvcmQgPSB7XG4gICAgICAgIGJyb3dzZXJfaWQsXG4gICAgICAgIHJlcXVlc3RfaWQsXG4gICAgICAgIGNhbGxfc3RhY2tcbiAgICAgIH07XG4gICAgICB0aGlzLmRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwiY2FsbHN0YWNrc1wiLCByZWNvcmQpO1xuICAgIH0pO1xuICB9XG59IiwiaW1wb3J0ICogYXMgc29ja2V0IGZyb20gXCIuL3NvY2tldC5qc1wiO1xuXG5sZXQgY3Jhd2xJRCA9IG51bGw7XG5sZXQgdmlzaXRJRCA9IG51bGw7XG5sZXQgZGVidWdnaW5nID0gZmFsc2U7XG5sZXQgc3RvcmFnZUNvbnRyb2xsZXIgPSBudWxsO1xubGV0IGxvZ0FnZ3JlZ2F0b3IgPSBudWxsO1xubGV0IGxpc3RlbmluZ1NvY2tldCA9IG51bGw7XG5cblxubGV0IGxpc3RlbmluZ1NvY2tldENhbGxiYWNrID0gIGFzeW5jIChkYXRhKSA9PiB7XG4gICAgLy9UaGlzIHdvcmtzIGV2ZW4gaWYgZGF0YSBpcyBhbiBpbnRcbiAgICBsZXQgYWN0aW9uID0gZGF0YVtcImFjdGlvblwiXTtcbiAgICBsZXQgX3Zpc2l0SUQgPSBkYXRhW1widmlzaXRfaWRcIl1cbiAgICBzd2l0Y2ggKGFjdGlvbikge1xuICAgICAgICBjYXNlIFwiSW5pdGlhbGl6ZVwiOlxuICAgICAgICAgICAgaWYgKHZpc2l0SUQpIHtcbiAgICAgICAgICAgICAgICBsb2dXYXJuKFwiU2V0IHZpc2l0X2lkIHdoaWxlIGFub3RoZXIgdmlzaXRfaWQgd2FzIHNldFwiKVxuICAgICAgICAgICAgfVxuICAgICAgICAgICAgdmlzaXRJRCA9IF92aXNpdElEO1xuICAgICAgICAgICAgZGF0YVtcImJyb3dzZXJfaWRcIl0gPSBjcmF3bElEO1xuICAgICAgICAgICAgc3RvcmFnZUNvbnRyb2xsZXIuc2VuZChKU09OLnN0cmluZ2lmeShbXCJtZXRhX2luZm9ybWF0aW9uXCIsIGRhdGFdKSk7XG4gICAgICAgICAgICBicmVhaztcbiAgICAgICAgY2FzZSBcIkZpbmFsaXplXCI6XG4gICAgICAgICAgICBpZiAoIXZpc2l0SUQpIHtcbiAgICAgICAgICAgICAgICBsb2dXYXJuKFwiU2VuZCBGaW5hbGl6ZSB3aGlsZSBubyB2aXNpdF9pZCB3YXMgc2V0XCIpXG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBpZiAoX3Zpc2l0SUQgIT09IHZpc2l0SUQgKSB7XG4gICAgICAgICAgICAgICAgbG9nRXJyb3IoXCJTZW5kIEZpbmFsaXplIGJ1dCB2aXNpdF9pZCBkaWRuJ3QgbWF0Y2guIFwiICtcbiAgICAgICAgICAgICAgICBgQ3VycmVudCB2aXNpdF9pZCAke3Zpc2l0X2lkfSwgc2VudCB2aXNpdF9pZCAke192aXNpdF9pZH0uYCk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBkYXRhW1wiYnJvd3Nlcl9pZFwiXSA9IGNyYXdsSUQ7XG4gICAgICAgICAgICBkYXRhW1wic3VjY2Vzc1wiXSA9IHRydWU7XG4gICAgICAgICAgICBzdG9yYWdlQ29udHJvbGxlci5zZW5kKEpTT04uc3RyaW5naWZ5KFtcIm1ldGFfaW5mb3JtYXRpb25cIiwgZGF0YV0pKTtcbiAgICAgICAgICAgIHZpc2l0SUQgPSBudWxsO1xuICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgICAvLyBKdXN0IG1ha2luZyBzdXJlIHRoYXQgaXQncyBhIHZhbGlkIG51bWJlciBiZWZvcmUgbG9nZ2luZ1xuICAgICAgICAgICAgX3Zpc2l0SUQgPSBwYXJzZUludChkYXRhLCAxMCk7XG4gICAgICAgICAgICBsb2dEZWJ1ZyhcIlNldHRpbmcgdmlzaXRfaWQgdGhlIGxlZ2FjeSB3YXlcIik7XG4gICAgICAgICAgICB2aXNpdElEID0gX3Zpc2l0SURcblxuICAgIH1cblxufVxuZXhwb3J0IGxldCBvcGVuID0gYXN5bmMgZnVuY3Rpb24oc3RvcmFnZUNvbnRyb2xsZXJBZGRyZXNzLCBsb2dBZGRyZXNzLCBjdXJyX2NyYXdsSUQpIHtcbiAgICBpZiAoc3RvcmFnZUNvbnRyb2xsZXJBZGRyZXNzID09IG51bGwgJiYgbG9nQWRkcmVzcyA9PSBudWxsICYmIGN1cnJfY3Jhd2xJRCA9PT0gMCkge1xuICAgICAgICBjb25zb2xlLmxvZyhcIkRlYnVnZ2luZywgZXZlcnl0aGluZyB3aWxsIG91dHB1dCB0byBjb25zb2xlXCIpO1xuICAgICAgICBkZWJ1Z2dpbmcgPSB0cnVlO1xuICAgICAgICByZXR1cm47XG4gICAgfVxuICAgIGNyYXdsSUQgPSBjdXJyX2NyYXdsSUQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIk9wZW5pbmcgc29ja2V0IGNvbm5lY3Rpb25zLi4uXCIpO1xuXG4gICAgLy8gQ29ubmVjdCB0byBNUExvZ2dlciBmb3IgZXh0ZW5zaW9uIGluZm8vZGVidWcvZXJyb3IgbG9nZ2luZ1xuICAgIGlmIChsb2dBZGRyZXNzICE9IG51bGwpIHtcbiAgICAgICAgbG9nQWdncmVnYXRvciA9IG5ldyBzb2NrZXQuU2VuZGluZ1NvY2tldCgpO1xuICAgICAgICBsZXQgcnYgPSBhd2FpdCBsb2dBZ2dyZWdhdG9yLmNvbm5lY3QobG9nQWRkcmVzc1swXSwgbG9nQWRkcmVzc1sxXSk7XG4gICAgICAgIGNvbnNvbGUubG9nKFwibG9nU29ja2V0IHN0YXJ0ZWQ/XCIsIHJ2KVxuICAgIH1cblxuICAgIC8vIENvbm5lY3QgdG8gZGF0YWJhc2VzIGZvciBzYXZpbmcgZGF0YVxuICAgIGlmIChzdG9yYWdlQ29udHJvbGxlckFkZHJlc3MgIT0gbnVsbCkge1xuICAgICAgICBzdG9yYWdlQ29udHJvbGxlciA9IG5ldyBzb2NrZXQuU2VuZGluZ1NvY2tldCgpO1xuICAgICAgICBsZXQgcnYgPSBhd2FpdCBzdG9yYWdlQ29udHJvbGxlci5jb25uZWN0KHN0b3JhZ2VDb250cm9sbGVyQWRkcmVzc1swXSwgc3RvcmFnZUNvbnRyb2xsZXJBZGRyZXNzWzFdKTtcbiAgICAgICAgY29uc29sZS5sb2coXCJTdG9yYWdlQ29udHJvbGxlciBzdGFydGVkP1wiLHJ2KTtcbiAgICB9XG5cbiAgICAvLyBMaXN0ZW4gZm9yIGluY29taW5nIHVybHMgYXMgdmlzaXQgaWRzXG4gICAgbGlzdGVuaW5nU29ja2V0ID0gbmV3IHNvY2tldC5MaXN0ZW5pbmdTb2NrZXQobGlzdGVuaW5nU29ja2V0Q2FsbGJhY2spO1xuICAgIGNvbnNvbGUubG9nKFwiU3RhcnRpbmcgc29ja2V0IGxpc3RlbmluZyBmb3IgaW5jb21pbmcgY29ubmVjdGlvbnMuXCIpO1xuICAgIGF3YWl0IGxpc3RlbmluZ1NvY2tldC5zdGFydExpc3RlbmluZygpLnRoZW4oKCkgPT4ge1xuICAgICAgICBicm93c2VyLnByb2ZpbGVEaXJJTy53cml0ZUZpbGUoXCJleHRlbnNpb25fcG9ydC50eHRcIiwgYCR7bGlzdGVuaW5nU29ja2V0LnBvcnR9YCk7XG4gICAgfSk7XG59O1xuXG5leHBvcnQgbGV0IGNsb3NlID0gZnVuY3Rpb24oKSB7XG4gICAgaWYgKHN0b3JhZ2VDb250cm9sbGVyICE9IG51bGwpIHtcbiAgICAgICAgc3RvcmFnZUNvbnRyb2xsZXIuY2xvc2UoKTtcbiAgICB9XG4gICAgaWYgKGxvZ0FnZ3JlZ2F0b3IgIT0gbnVsbCkge1xuICAgICAgICBsb2dBZ2dyZWdhdG9yLmNsb3NlKCk7XG4gICAgfVxufTtcblxubGV0IG1ha2VMb2dKU09OID0gZnVuY3Rpb24obHZsLCBtc2cpIHtcbiAgICB2YXIgbG9nX2pzb24gPSB7XG4gICAgICAgICduYW1lJzogJ0V4dGVuc2lvbi1Mb2dnZXInLFxuICAgICAgICAnbGV2ZWwnOiBsdmwsXG4gICAgICAgICdwYXRobmFtZSc6ICdGaXJlZm94RXh0ZW5zaW9uJyxcbiAgICAgICAgJ2xpbmVubyc6IDEsXG4gICAgICAgICdtc2cnOiBlc2NhcGVTdHJpbmcobXNnKSxcbiAgICAgICAgJ2FyZ3MnOiBudWxsLFxuICAgICAgICAnZXhjX2luZm8nOiBudWxsLFxuICAgICAgICAnZnVuYyc6IG51bGxcbiAgICB9XG4gICAgcmV0dXJuIGxvZ19qc29uO1xufVxuXG5leHBvcnQgbGV0IGxvZ0luZm8gPSBmdW5jdGlvbihtc2cpIHtcbiAgICAvLyBBbHdheXMgbG9nIHRvIGJyb3dzZXIgY29uc29sZVxuICAgIGNvbnNvbGUubG9nKG1zZyk7XG5cbiAgICBpZiAoZGVidWdnaW5nKSB7XG4gICAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICAvLyBMb2cgbGV2ZWwgSU5GTyA9PSAyMCAoaHR0cHM6Ly9kb2NzLnB5dGhvbi5vcmcvMi9saWJyYXJ5L2xvZ2dpbmcuaHRtbCNsb2dnaW5nLWxldmVscylcbiAgICB2YXIgbG9nX2pzb24gPSBtYWtlTG9nSlNPTigyMCwgbXNnKTtcbiAgICBsb2dBZ2dyZWdhdG9yLnNlbmQoSlNPTi5zdHJpbmdpZnkoWydFWFQnLCBKU09OLnN0cmluZ2lmeShsb2dfanNvbildKSk7XG59O1xuXG5leHBvcnQgbGV0IGxvZ0RlYnVnID0gZnVuY3Rpb24obXNnKSB7XG4gICAgLy8gQWx3YXlzIGxvZyB0byBicm93c2VyIGNvbnNvbGVcbiAgICBjb25zb2xlLmxvZyhtc2cpO1xuXG4gICAgaWYgKGRlYnVnZ2luZykge1xuICAgICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgLy8gTG9nIGxldmVsIERFQlVHID09IDEwIChodHRwczovL2RvY3MucHl0aG9uLm9yZy8yL2xpYnJhcnkvbG9nZ2luZy5odG1sI2xvZ2dpbmctbGV2ZWxzKVxuICAgIHZhciBsb2dfanNvbiA9IG1ha2VMb2dKU09OKDEwLCBtc2cpO1xuICAgIGxvZ0FnZ3JlZ2F0b3Iuc2VuZChKU09OLnN0cmluZ2lmeShbJ0VYVCcsIEpTT04uc3RyaW5naWZ5KGxvZ19qc29uKV0pKTtcbn07XG5cbmV4cG9ydCBsZXQgbG9nV2FybiA9IGZ1bmN0aW9uKG1zZykge1xuICAgIC8vIEFsd2F5cyBsb2cgdG8gYnJvd3NlciBjb25zb2xlXG4gICAgY29uc29sZS53YXJuKG1zZyk7XG5cbiAgICBpZiAoZGVidWdnaW5nKSB7XG4gICAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICAvLyBMb2cgbGV2ZWwgV0FSTiA9PSAzMCAoaHR0cHM6Ly9kb2NzLnB5dGhvbi5vcmcvMi9saWJyYXJ5L2xvZ2dpbmcuaHRtbCNsb2dnaW5nLWxldmVscylcbiAgICB2YXIgbG9nX2pzb24gPSBtYWtlTG9nSlNPTigzMCwgbXNnKTtcbiAgICBsb2dBZ2dyZWdhdG9yLnNlbmQoSlNPTi5zdHJpbmdpZnkoWydFWFQnLCBKU09OLnN0cmluZ2lmeShsb2dfanNvbildKSk7XG59O1xuXG5leHBvcnQgbGV0IGxvZ0Vycm9yID0gZnVuY3Rpb24obXNnKSB7XG4gICAgLy8gQWx3YXlzIGxvZyB0byBicm93c2VyIGNvbnNvbGVcbiAgICBjb25zb2xlLmVycm9yKG1zZyk7XG5cbiAgICBpZiAoZGVidWdnaW5nKSB7XG4gICAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICAvLyBMb2cgbGV2ZWwgSU5GTyA9PSA0MCAoaHR0cHM6Ly9kb2NzLnB5dGhvbi5vcmcvMi9saWJyYXJ5L2xvZ2dpbmcuaHRtbCNsb2dnaW5nLWxldmVscylcbiAgICB2YXIgbG9nX2pzb24gPSBtYWtlTG9nSlNPTig0MCwgbXNnKTtcbiAgICBsb2dBZ2dyZWdhdG9yLnNlbmQoSlNPTi5zdHJpbmdpZnkoWydFWFQnLCBKU09OLnN0cmluZ2lmeShsb2dfanNvbildKSk7XG59O1xuXG5leHBvcnQgbGV0IGxvZ0NyaXRpY2FsID0gZnVuY3Rpb24obXNnKSB7XG4gICAgLy8gQWx3YXlzIGxvZyB0byBicm93c2VyIGNvbnNvbGVcbiAgICBjb25zb2xlLmVycm9yKG1zZyk7XG5cbiAgICBpZiAoZGVidWdnaW5nKSB7XG4gICAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICAvLyBMb2cgbGV2ZWwgQ1JJVElDQUwgPT0gNTAgKGh0dHBzOi8vZG9jcy5weXRob24ub3JnLzIvbGlicmFyeS9sb2dnaW5nLmh0bWwjbG9nZ2luZy1sZXZlbHMpXG4gICAgdmFyIGxvZ19qc29uID0gbWFrZUxvZ0pTT04oNTAsIG1zZyk7XG4gICAgbG9nQWdncmVnYXRvci5zZW5kKEpTT04uc3RyaW5naWZ5KFsnRVhUJywgSlNPTi5zdHJpbmdpZnkobG9nX2pzb24pXSkpO1xufTtcblxuZXhwb3J0IGxldCBkYXRhUmVjZWl2ZXIgPSB7XG4gICAgc2F2ZVJlY29yZChhLCBiKSB7XG4gICAgICAgIGNvbnNvbGUubG9nKGIpO1xuICAgIH0sXG59O1xuXG5leHBvcnQgbGV0IHNhdmVSZWNvcmQgPSBmdW5jdGlvbihpbnN0cnVtZW50LCByZWNvcmQpIHtcbiAgICByZWNvcmRbXCJ2aXNpdF9pZFwiXSA9IHZpc2l0SUQ7XG5cbiAgICBpZiAoIXZpc2l0SUQgJiYgIWRlYnVnZ2luZykge1xuICAgICAgICAvLyBOYXZpZ2F0aW9ucyB0byBhYm91dDpibGFuayBjYW4gYmUgdHJpZ2dlcmVkIGJ5IE9wZW5XUE0uIFdlIGRyb3AgdGhvc2UuXG4gICAgICAgIGlmKGluc3RydW1lbnQgPT09ICduYXZpZ2F0aW9ucycgJiYgcmVjb3JkWyd1cmwnXSA9PT0gJ2Fib3V0OmJsYW5rJykge1xuICAgICAgICAgICAgbG9nRGVidWcoJ0V4dGVuc2lvbi0nICsgY3Jhd2xJRCArICcgOiBEcm9wcGluZyBuYXZpZ2F0aW9uIHRvIGFib3V0OmJsYW5rIGluIGludGVybWVkaWF0ZSBwZXJpb2QnKTtcbiAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgfVxuICAgICAgICBsb2dXYXJuKGBFeHRlbnNpb24tJHtjcmF3bElEfSA6IHZpc2l0SUQgaXMgbnVsbCB3aGlsZSBhdHRlbXB0aW5nIHRvIGluc2VydCBpbnRvIHRhYmxlICR7aW5zdHJ1bWVudH1cXG5gICtcbiAgICAgICAgICAgICAgICAgICAgSlNPTi5zdHJpbmdpZnkocmVjb3JkKSk7XG4gICAgICAgIHJlY29yZFtcInZpc2l0X2lkXCJdID0gLTE7XG4gICAgICAgIFxuICAgIH1cblxuICAgIC8vIHNlbmQgdG8gY29uc29sZSBpZiBkZWJ1Z2dpbmdcbiAgICBpZiAoZGVidWdnaW5nKSB7XG4gICAgICBjb25zb2xlLmxvZyhcIkVYVEVOU0lPTlwiLCBpbnN0cnVtZW50LCByZWNvcmQpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBzdG9yYWdlQ29udHJvbGxlci5zZW5kKEpTT04uc3RyaW5naWZ5KFtpbnN0cnVtZW50LCByZWNvcmRdKSk7XG59O1xuXG4vLyBTdHViIGZvciBub3dcbmV4cG9ydCBsZXQgc2F2ZUNvbnRlbnQgPSBhc3luYyBmdW5jdGlvbihjb250ZW50LCBjb250ZW50SGFzaCkge1xuICAvLyBTZW5kIHBhZ2UgY29udGVudCB0byB0aGUgZGF0YSBhZ2dyZWdhdG9yXG4gIC8vIGRlZHVwbGljYXRlZCBieSBjb250ZW50SGFzaCBpbiBhIGxldmVsREIgZGF0YWJhc2VcbiAgaWYgKGRlYnVnZ2luZykge1xuICAgIGNvbnNvbGUubG9nKFwiTERCIGNvbnRlbnRIYXNoOlwiLGNvbnRlbnRIYXNoLFwid2l0aCBsZW5ndGhcIixjb250ZW50Lmxlbmd0aCk7XG4gICAgcmV0dXJuO1xuICB9XG4gIC8vIFNpbmNlIHRoZSBjb250ZW50IG1pZ2h0IG5vdCBiZSBhIHZhbGlkIHV0Zjggc3RyaW5nIGFuZCBpdCBuZWVkcyB0byBiZVxuICAvLyBqc29uIGVuY29kZWQgbGF0ZXIsIGl0IGlzIGVuY29kZWQgdXNpbmcgYmFzZTY0IGZpcnN0LlxuICBjb25zdCBiNjQgPSBVaW50OFRvQmFzZTY0KGNvbnRlbnQpO1xuICBzdG9yYWdlQ29udHJvbGxlci5zZW5kKEpTT04uc3RyaW5naWZ5KFsncGFnZV9jb250ZW50JywgW2I2NCwgY29udGVudEhhc2hdXSkpO1xufTtcblxuZnVuY3Rpb24gZW5jb2RlX3V0Zjgocykge1xuICByZXR1cm4gdW5lc2NhcGUoZW5jb2RlVVJJQ29tcG9uZW50KHMpKTtcbn1cblxuLy8gQmFzZTY0IGVuY29kaW5nLCBmb3VuZCBvbjpcbi8vIGh0dHBzOi8vc3RhY2tvdmVyZmxvdy5jb20vcXVlc3Rpb25zLzEyNzEwMDAxL2hvdy10by1jb252ZXJ0LXVpbnQ4LWFycmF5LXRvLWJhc2U2NC1lbmNvZGVkLXN0cmluZy8yNTY0NDQwOSMyNTY0NDQwOVxuZnVuY3Rpb24gVWludDhUb0Jhc2U2NCh1OEFycil7XG4gIHZhciBDSFVOS19TSVpFID0gMHg4MDAwOyAvL2FyYml0cmFyeSBudW1iZXJcbiAgdmFyIGluZGV4ID0gMDtcbiAgdmFyIGxlbmd0aCA9IHU4QXJyLmxlbmd0aDtcbiAgdmFyIHJlc3VsdCA9ICcnO1xuICB2YXIgc2xpY2U7XG4gIHdoaWxlIChpbmRleCA8IGxlbmd0aCkge1xuICAgIHNsaWNlID0gdThBcnIuc3ViYXJyYXkoaW5kZXgsIE1hdGgubWluKGluZGV4ICsgQ0hVTktfU0laRSwgbGVuZ3RoKSk7XG4gICAgcmVzdWx0ICs9IFN0cmluZy5mcm9tQ2hhckNvZGUuYXBwbHkobnVsbCwgc2xpY2UpO1xuICAgIGluZGV4ICs9IENIVU5LX1NJWkU7XG4gIH1cbiAgcmV0dXJuIGJ0b2EocmVzdWx0KTtcbn1cblxuZXhwb3J0IGxldCBlc2NhcGVTdHJpbmcgPSBmdW5jdGlvbihzdHJpbmcpIHtcbiAgICAvLyBDb252ZXJ0IHRvIHN0cmluZyBpZiBuZWNlc3NhcnlcbiAgICBpZih0eXBlb2Ygc3RyaW5nICE9IFwic3RyaW5nXCIpXG4gICAgICAgIHN0cmluZyA9IFwiXCIgKyBzdHJpbmc7XG5cbiAgICByZXR1cm4gZW5jb2RlX3V0Zjgoc3RyaW5nKTtcbn07XG5cbmV4cG9ydCBsZXQgYm9vbFRvSW50ID0gZnVuY3Rpb24oYm9vbCkge1xuICAgIHJldHVybiBib29sID8gMSA6IDA7XG59O1xuIiwibGV0IERhdGFSZWNlaXZlciA9IHtcbiAgY2FsbGJhY2tzOiBuZXcgTWFwKCksXG4gIG9uRGF0YVJlY2VpdmVkOiAoYVNvY2tldElkLCBhRGF0YSwgYUpTT04pID0+IHtcbiAgICBpZiAoIURhdGFSZWNlaXZlci5jYWxsYmFja3MuaGFzKGFTb2NrZXRJZCkpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgaWYgKGFKU09OKSB7XG4gICAgICBhRGF0YSA9IEpTT04ucGFyc2UoYURhdGEpO1xuICAgIH1cbiAgICBEYXRhUmVjZWl2ZXIuY2FsbGJhY2tzLmdldChhU29ja2V0SWQpKGFEYXRhKTtcbiAgfSxcbn07XG5cbmJyb3dzZXIuc29ja2V0cy5vbkRhdGFSZWNlaXZlZC5hZGRMaXN0ZW5lcihEYXRhUmVjZWl2ZXIub25EYXRhUmVjZWl2ZWQpO1xuXG5sZXQgTGlzdGVuaW5nU29ja2V0cyA9IG5ldyBNYXAoKTtcblxuZXhwb3J0IGNsYXNzIExpc3RlbmluZ1NvY2tldCB7XG4gIGNvbnN0cnVjdG9yKGNhbGxiYWNrKSB7XG4gICAgdGhpcy5jYWxsYmFjayA9IGNhbGxiYWNrXG4gIH1cblxuICBhc3luYyBzdGFydExpc3RlbmluZygpIHtcbiAgICB0aGlzLnBvcnQgPSBhd2FpdCBicm93c2VyLnNvY2tldHMuY3JlYXRlU2VydmVyU29ja2V0KCk7XG4gICAgRGF0YVJlY2VpdmVyLmNhbGxiYWNrcy5zZXQodGhpcy5wb3J0LCB0aGlzLmNhbGxiYWNrKTtcbiAgICBicm93c2VyLnNvY2tldHMuc3RhcnRMaXN0ZW5pbmcodGhpcy5wb3J0KTtcbiAgICBjb25zb2xlLmxvZygnTGlzdGVuaW5nIG9uIHBvcnQgJyArIHRoaXMucG9ydCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFNlbmRpbmdTb2NrZXQge1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgfVxuXG4gIGFzeW5jIGNvbm5lY3QoaG9zdCwgcG9ydCkge1xuICAgIHRoaXMuaWQgPSBhd2FpdCBicm93c2VyLnNvY2tldHMuY3JlYXRlU2VuZGluZ1NvY2tldCgpO1xuICAgIGJyb3dzZXIuc29ja2V0cy5jb25uZWN0KHRoaXMuaWQsIGhvc3QsIHBvcnQpO1xuICAgIGNvbnNvbGUubG9nKGBDb25uZWN0ZWQgdG8gJHtob3N0fToke3BvcnR9YCk7XG4gIH1cblxuICBzZW5kKGFEYXRhLCBhSlNPTj10cnVlKSB7XG4gICAgdHJ5IHtcbiAgICAgIGJyb3dzZXIuc29ja2V0cy5zZW5kRGF0YSh0aGlzLmlkLCBhRGF0YSwgISFhSlNPTik7XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoZXJyLGVyci5tZXNzYWdlKTtcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgICB9XG4gIH1cblxuICBjbG9zZSgpIHtcbiAgICBicm93c2VyLnNvY2tldHMuY2xvc2UodGhpcy5pZCk7XG4gIH1cbn1cblxuIiwiaW1wb3J0IHsgaW5jcmVtZW50ZWRFdmVudE9yZGluYWwgfSBmcm9tIFwiLi4vbGliL2V4dGVuc2lvbi1zZXNzaW9uLWV2ZW50LW9yZGluYWxcIjtcbmltcG9ydCB7IGV4dGVuc2lvblNlc3Npb25VdWlkIH0gZnJvbSBcIi4uL2xpYi9leHRlbnNpb24tc2Vzc2lvbi11dWlkXCI7XG5pbXBvcnQgeyBib29sVG9JbnQsIGVzY2FwZVN0cmluZyB9IGZyb20gXCIuLi9saWIvc3RyaW5nLXV0aWxzXCI7XG5leHBvcnQgY29uc3QgdHJhbnNmb3JtQ29va2llT2JqZWN0VG9NYXRjaE9wZW5XUE1TY2hlbWEgPSAoY29va2llKSA9PiB7XG4gICAgY29uc3QgamF2YXNjcmlwdENvb2tpZSA9IHt9O1xuICAgIC8vIEV4cGlyeSB0aW1lIChpbiBzZWNvbmRzKVxuICAgIC8vIE1heSByZXR1cm4gfk1heChpbnQ2NCkuIEkgYmVsaWV2ZSB0aGlzIGlzIGEgc2Vzc2lvblxuICAgIC8vIGNvb2tpZSB3aGljaCBkb2Vzbid0IGV4cGlyZS4gU2Vzc2lvbnMgY29va2llcyB3aXRoXG4gICAgLy8gbm9uLW1heCBleHBpcnkgdGltZSBleHBpcmUgYWZ0ZXIgc2Vzc2lvbiBvciBhdCBleHBpcnkuXG4gICAgY29uc3QgZXhwaXJ5VGltZSA9IGNvb2tpZS5leHBpcmF0aW9uRGF0ZTsgLy8gcmV0dXJucyBzZWNvbmRzXG4gICAgbGV0IGV4cGlyeVRpbWVTdHJpbmc7XG4gICAgY29uc3QgbWF4SW50NjQgPSA5MjIzMzcyMDM2ODU0Nzc2MDAwO1xuICAgIGlmICghY29va2llLmV4cGlyYXRpb25EYXRlIHx8IGV4cGlyeVRpbWUgPT09IG1heEludDY0KSB7XG4gICAgICAgIGV4cGlyeVRpbWVTdHJpbmcgPSBcIjk5OTktMTItMzFUMjE6NTk6NTkuMDAwWlwiO1xuICAgIH1cbiAgICBlbHNlIHtcbiAgICAgICAgY29uc3QgZXhwaXJ5VGltZURhdGUgPSBuZXcgRGF0ZShleHBpcnlUaW1lICogMTAwMCk7IC8vIHJlcXVpcmVzIG1pbGxpc2Vjb25kc1xuICAgICAgICBleHBpcnlUaW1lU3RyaW5nID0gZXhwaXJ5VGltZURhdGUudG9JU09TdHJpbmcoKTtcbiAgICB9XG4gICAgamF2YXNjcmlwdENvb2tpZS5leHBpcnkgPSBleHBpcnlUaW1lU3RyaW5nO1xuICAgIGphdmFzY3JpcHRDb29raWUuaXNfaHR0cF9vbmx5ID0gYm9vbFRvSW50KGNvb2tpZS5odHRwT25seSk7XG4gICAgamF2YXNjcmlwdENvb2tpZS5pc19ob3N0X29ubHkgPSBib29sVG9JbnQoY29va2llLmhvc3RPbmx5KTtcbiAgICBqYXZhc2NyaXB0Q29va2llLmlzX3Nlc3Npb24gPSBib29sVG9JbnQoY29va2llLnNlc3Npb24pO1xuICAgIGphdmFzY3JpcHRDb29raWUuaG9zdCA9IGVzY2FwZVN0cmluZyhjb29raWUuZG9tYWluKTtcbiAgICBqYXZhc2NyaXB0Q29va2llLmlzX3NlY3VyZSA9IGJvb2xUb0ludChjb29raWUuc2VjdXJlKTtcbiAgICBqYXZhc2NyaXB0Q29va2llLm5hbWUgPSBlc2NhcGVTdHJpbmcoY29va2llLm5hbWUpO1xuICAgIGphdmFzY3JpcHRDb29raWUucGF0aCA9IGVzY2FwZVN0cmluZyhjb29raWUucGF0aCk7XG4gICAgamF2YXNjcmlwdENvb2tpZS52YWx1ZSA9IGVzY2FwZVN0cmluZyhjb29raWUudmFsdWUpO1xuICAgIGphdmFzY3JpcHRDb29raWUuc2FtZV9zaXRlID0gZXNjYXBlU3RyaW5nKGNvb2tpZS5zYW1lU2l0ZSk7XG4gICAgamF2YXNjcmlwdENvb2tpZS5maXJzdF9wYXJ0eV9kb21haW4gPSBlc2NhcGVTdHJpbmcoY29va2llLmZpcnN0UGFydHlEb21haW4pO1xuICAgIGphdmFzY3JpcHRDb29raWUuc3RvcmVfaWQgPSBlc2NhcGVTdHJpbmcoY29va2llLnN0b3JlSWQpO1xuICAgIGphdmFzY3JpcHRDb29raWUudGltZV9zdGFtcCA9IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKTtcbiAgICByZXR1cm4gamF2YXNjcmlwdENvb2tpZTtcbn07XG5leHBvcnQgY2xhc3MgQ29va2llSW5zdHJ1bWVudCB7XG4gICAgZGF0YVJlY2VpdmVyO1xuICAgIG9uQ2hhbmdlZExpc3RlbmVyO1xuICAgIGNvbnN0cnVjdG9yKGRhdGFSZWNlaXZlcikge1xuICAgICAgICB0aGlzLmRhdGFSZWNlaXZlciA9IGRhdGFSZWNlaXZlcjtcbiAgICB9XG4gICAgcnVuKGNyYXdsSUQpIHtcbiAgICAgICAgLy8gSW5zdHJ1bWVudCBjb29raWUgY2hhbmdlc1xuICAgICAgICB0aGlzLm9uQ2hhbmdlZExpc3RlbmVyID0gYXN5bmMgKGNoYW5nZUluZm8pID0+IHtcbiAgICAgICAgICAgIGNvbnN0IGV2ZW50VHlwZSA9IGNoYW5nZUluZm8ucmVtb3ZlZCA/IFwiZGVsZXRlZFwiIDogXCJhZGRlZC1vci1jaGFuZ2VkXCI7XG4gICAgICAgICAgICBjb25zdCB1cGRhdGUgPSB7XG4gICAgICAgICAgICAgICAgcmVjb3JkX3R5cGU6IGV2ZW50VHlwZSxcbiAgICAgICAgICAgICAgICBjaGFuZ2VfY2F1c2U6IGNoYW5nZUluZm8uY2F1c2UsXG4gICAgICAgICAgICAgICAgYnJvd3Nlcl9pZDogY3Jhd2xJRCxcbiAgICAgICAgICAgICAgICBleHRlbnNpb25fc2Vzc2lvbl91dWlkOiBleHRlbnNpb25TZXNzaW9uVXVpZCxcbiAgICAgICAgICAgICAgICBldmVudF9vcmRpbmFsOiBpbmNyZW1lbnRlZEV2ZW50T3JkaW5hbCgpLFxuICAgICAgICAgICAgICAgIC4uLnRyYW5zZm9ybUNvb2tpZU9iamVjdFRvTWF0Y2hPcGVuV1BNU2NoZW1hKGNoYW5nZUluZm8uY29va2llKSxcbiAgICAgICAgICAgIH07XG4gICAgICAgICAgICB0aGlzLmRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwiamF2YXNjcmlwdF9jb29raWVzXCIsIHVwZGF0ZSk7XG4gICAgICAgIH07XG4gICAgICAgIGJyb3dzZXIuY29va2llcy5vbkNoYW5nZWQuYWRkTGlzdGVuZXIodGhpcy5vbkNoYW5nZWRMaXN0ZW5lcik7XG4gICAgfVxuICAgIGFzeW5jIHNhdmVBbGxDb29raWVzKGNyYXdsSUQpIHtcbiAgICAgICAgY29uc3QgYWxsQ29va2llcyA9IGF3YWl0IGJyb3dzZXIuY29va2llcy5nZXRBbGwoe30pO1xuICAgICAgICBhd2FpdCBQcm9taXNlLmFsbChhbGxDb29raWVzLm1hcCgoY29va2llKSA9PiB7XG4gICAgICAgICAgICBjb25zdCB1cGRhdGUgPSB7XG4gICAgICAgICAgICAgICAgcmVjb3JkX3R5cGU6IFwibWFudWFsLWV4cG9ydFwiLFxuICAgICAgICAgICAgICAgIGJyb3dzZXJfaWQ6IGNyYXdsSUQsXG4gICAgICAgICAgICAgICAgZXh0ZW5zaW9uX3Nlc3Npb25fdXVpZDogZXh0ZW5zaW9uU2Vzc2lvblV1aWQsXG4gICAgICAgICAgICAgICAgLi4udHJhbnNmb3JtQ29va2llT2JqZWN0VG9NYXRjaE9wZW5XUE1TY2hlbWEoY29va2llKSxcbiAgICAgICAgICAgIH07XG4gICAgICAgICAgICByZXR1cm4gdGhpcy5kYXRhUmVjZWl2ZXIuc2F2ZVJlY29yZChcImphdmFzY3JpcHRfY29va2llc1wiLCB1cGRhdGUpO1xuICAgICAgICB9KSk7XG4gICAgfVxuICAgIGNsZWFudXAoKSB7XG4gICAgICAgIGlmICh0aGlzLm9uQ2hhbmdlZExpc3RlbmVyKSB7XG4gICAgICAgICAgICBicm93c2VyLmNvb2tpZXMub25DaGFuZ2VkLnJlbW92ZUxpc3RlbmVyKHRoaXMub25DaGFuZ2VkTGlzdGVuZXIpO1xuICAgICAgICB9XG4gICAgfVxufVxuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pWTI5dmEybGxMV2x1YzNSeWRXMWxiblF1YW5NaUxDSnpiM1Z5WTJWU2IyOTBJam9pSWl3aWMyOTFjbU5sY3lJNld5SXVMaTh1TGk4dUxpOXpjbU12WW1GamEyZHliM1Z1WkM5amIyOXJhV1V0YVc1emRISjFiV1Z1ZEM1MGN5SmRMQ0p1WVcxbGN5STZXMTBzSW0xaGNIQnBibWR6SWpvaVFVRkJRU3hQUVVGUExFVkJRVVVzZFVKQlFYVkNMRVZCUVVVc1RVRkJUU3gzUTBGQmQwTXNRMEZCUXp0QlFVTnFSaXhQUVVGUExFVkJRVVVzYjBKQlFXOUNMRVZCUVVVc1RVRkJUU3dyUWtGQkswSXNRMEZCUXp0QlFVTnlSU3hQUVVGUExFVkJRVVVzVTBGQlV5eEZRVUZGTEZsQlFWa3NSVUZCUlN4TlFVRk5MSEZDUVVGeFFpeERRVUZETzBGQlN6bEVMRTFCUVUwc1EwRkJReXhOUVVGTkxIbERRVUY1UXl4SFFVRkhMRU5CUVVNc1RVRkJZeXhGUVVGRkxFVkJRVVU3U1VGRE1VVXNUVUZCVFN4blFrRkJaMElzUjBGQlJ5eEZRVUZ6UWl4RFFVRkRPMGxCUldoRUxESkNRVUV5UWp0SlFVTXpRaXh6UkVGQmMwUTdTVUZEZEVRc2NVUkJRWEZFTzBsQlEzSkVMSGxFUVVGNVJEdEpRVU42UkN4TlFVRk5MRlZCUVZVc1IwRkJSeXhOUVVGTkxFTkJRVU1zWTBGQll5eERRVUZETEVOQlFVTXNhMEpCUVd0Q08wbEJRelZFTEVsQlFVa3NaMEpCUVdkQ0xFTkJRVU03U1VGRGNrSXNUVUZCVFN4UlFVRlJMRWRCUVVjc2JVSkJRVzFDTEVOQlFVTTdTVUZEY2tNc1NVRkJTU3hEUVVGRExFMUJRVTBzUTBGQlF5eGpRVUZqTEVsQlFVa3NWVUZCVlN4TFFVRkxMRkZCUVZFc1JVRkJSVHRSUVVOeVJDeG5Ra0ZCWjBJc1IwRkJSeXd3UWtGQk1FSXNRMEZCUXp0TFFVTXZRenRUUVVGTk8xRkJRMHdzVFVGQlRTeGpRVUZqTEVkQlFVY3NTVUZCU1N4SlFVRkpMRU5CUVVNc1ZVRkJWU3hIUVVGSExFbEJRVWtzUTBGQlF5eERRVUZETEVOQlFVTXNkMEpCUVhkQ08xRkJRelZGTEdkQ1FVRm5RaXhIUVVGSExHTkJRV01zUTBGQlF5eFhRVUZYTEVWQlFVVXNRMEZCUXp0TFFVTnFSRHRKUVVORUxHZENRVUZuUWl4RFFVRkRMRTFCUVUwc1IwRkJSeXhuUWtGQlowSXNRMEZCUXp0SlFVTXpReXhuUWtGQlowSXNRMEZCUXl4WlFVRlpMRWRCUVVjc1UwRkJVeXhEUVVGRExFMUJRVTBzUTBGQlF5eFJRVUZSTEVOQlFVTXNRMEZCUXp0SlFVTXpSQ3huUWtGQlowSXNRMEZCUXl4WlFVRlpMRWRCUVVjc1UwRkJVeXhEUVVGRExFMUJRVTBzUTBGQlF5eFJRVUZSTEVOQlFVTXNRMEZCUXp0SlFVTXpSQ3huUWtGQlowSXNRMEZCUXl4VlFVRlZMRWRCUVVjc1UwRkJVeXhEUVVGRExFMUJRVTBzUTBGQlF5eFBRVUZQTEVOQlFVTXNRMEZCUXp0SlFVVjRSQ3huUWtGQlowSXNRMEZCUXl4SlFVRkpMRWRCUVVjc1dVRkJXU3hEUVVGRExFMUJRVTBzUTBGQlF5eE5RVUZOTEVOQlFVTXNRMEZCUXp0SlFVTndSQ3huUWtGQlowSXNRMEZCUXl4VFFVRlRMRWRCUVVjc1UwRkJVeXhEUVVGRExFMUJRVTBzUTBGQlF5eE5RVUZOTEVOQlFVTXNRMEZCUXp0SlFVTjBSQ3huUWtGQlowSXNRMEZCUXl4SlFVRkpMRWRCUVVjc1dVRkJXU3hEUVVGRExFMUJRVTBzUTBGQlF5eEpRVUZKTEVOQlFVTXNRMEZCUXp0SlFVTnNSQ3huUWtGQlowSXNRMEZCUXl4SlFVRkpMRWRCUVVjc1dVRkJXU3hEUVVGRExFMUJRVTBzUTBGQlF5eEpRVUZKTEVOQlFVTXNRMEZCUXp0SlFVTnNSQ3huUWtGQlowSXNRMEZCUXl4TFFVRkxMRWRCUVVjc1dVRkJXU3hEUVVGRExFMUJRVTBzUTBGQlF5eExRVUZMTEVOQlFVTXNRMEZCUXp0SlFVTndSQ3huUWtGQlowSXNRMEZCUXl4VFFVRlRMRWRCUVVjc1dVRkJXU3hEUVVGRExFMUJRVTBzUTBGQlF5eFJRVUZSTEVOQlFVTXNRMEZCUXp0SlFVTXpSQ3huUWtGQlowSXNRMEZCUXl4clFrRkJhMElzUjBGQlJ5eFpRVUZaTEVOQlFVTXNUVUZCVFN4RFFVRkRMR2RDUVVGblFpeERRVUZETEVOQlFVTTdTVUZETlVVc1owSkJRV2RDTEVOQlFVTXNVVUZCVVN4SFFVRkhMRmxCUVZrc1EwRkJReXhOUVVGTkxFTkJRVU1zVDBGQlR5eERRVUZETEVOQlFVTTdTVUZGZWtRc1owSkJRV2RDTEVOQlFVTXNWVUZCVlN4SFFVRkhMRWxCUVVrc1NVRkJTU3hGUVVGRkxFTkJRVU1zVjBGQlZ5eEZRVUZGTEVOQlFVTTdTVUZGZGtRc1QwRkJUeXhuUWtGQlowSXNRMEZCUXp0QlFVTXhRaXhEUVVGRExFTkJRVU03UVVGRlJpeE5RVUZOTEU5QlFVOHNaMEpCUVdkQ08wbEJRMVlzV1VGQldTeERRVUZETzBsQlEzUkNMR2xDUVVGcFFpeERRVUZETzBsQlJURkNMRmxCUVZrc1dVRkJXVHRSUVVOMFFpeEpRVUZKTEVOQlFVTXNXVUZCV1N4SFFVRkhMRmxCUVZrc1EwRkJRenRKUVVOdVF5eERRVUZETzBsQlJVMHNSMEZCUnl4RFFVRkRMRTlCUVU4N1VVRkRhRUlzTkVKQlFUUkNPMUZCUXpWQ0xFbEJRVWtzUTBGQlF5eHBRa0ZCYVVJc1IwRkJSeXhMUVVGTExFVkJRVVVzVlVGUEwwSXNSVUZCUlN4RlFVRkZPMWxCUTBnc1RVRkJUU3hUUVVGVExFZEJRVWNzVlVGQlZTeERRVUZETEU5QlFVOHNRMEZCUXl4RFFVRkRMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU1zUTBGQlF5eHJRa0ZCYTBJc1EwRkJRenRaUVVOMFJTeE5RVUZOTEUxQlFVMHNSMEZCTWtJN1owSkJRM0pETEZkQlFWY3NSVUZCUlN4VFFVRlRPMmRDUVVOMFFpeFpRVUZaTEVWQlFVVXNWVUZCVlN4RFFVRkRMRXRCUVVzN1owSkJRemxDTEZWQlFWVXNSVUZCUlN4UFFVRlBPMmRDUVVOdVFpeHpRa0ZCYzBJc1JVRkJSU3h2UWtGQmIwSTdaMEpCUXpWRExHRkJRV0VzUlVGQlJTeDFRa0ZCZFVJc1JVRkJSVHRuUWtGRGVFTXNSMEZCUnl4NVEwRkJlVU1zUTBGQlF5eFZRVUZWTEVOQlFVTXNUVUZCVFN4RFFVRkRPMkZCUTJoRkxFTkJRVU03V1VGRFJpeEpRVUZKTEVOQlFVTXNXVUZCV1N4RFFVRkRMRlZCUVZVc1EwRkJReXh2UWtGQmIwSXNSVUZCUlN4TlFVRk5MRU5CUVVNc1EwRkJRenRSUVVNM1JDeERRVUZETEVOQlFVTTdVVUZEUml4UFFVRlBMRU5CUVVNc1QwRkJUeXhEUVVGRExGTkJRVk1zUTBGQlF5eFhRVUZYTEVOQlFVTXNTVUZCU1N4RFFVRkRMR2xDUVVGcFFpeERRVUZETEVOQlFVTTdTVUZEYUVVc1EwRkJRenRKUVVWTkxFdEJRVXNzUTBGQlF5eGpRVUZqTEVOQlFVTXNUMEZCVHp0UlFVTnFReXhOUVVGTkxGVkJRVlVzUjBGQlJ5eE5RVUZOTEU5QlFVOHNRMEZCUXl4UFFVRlBMRU5CUVVNc1RVRkJUU3hEUVVGRExFVkJRVVVzUTBGQlF5eERRVUZETzFGQlEzQkVMRTFCUVUwc1QwRkJUeXhEUVVGRExFZEJRVWNzUTBGRFppeFZRVUZWTEVOQlFVTXNSMEZCUnl4RFFVRkRMRU5CUVVNc1RVRkJZeXhGUVVGRkxFVkJRVVU3V1VGRGFFTXNUVUZCVFN4TlFVRk5MRWRCUVRKQ08yZENRVU55UXl4WFFVRlhMRVZCUVVVc1pVRkJaVHRuUWtGRE5VSXNWVUZCVlN4RlFVRkZMRTlCUVU4N1owSkJRMjVDTEhOQ1FVRnpRaXhGUVVGRkxHOUNRVUZ2UWp0blFrRkROVU1zUjBGQlJ5eDVRMEZCZVVNc1EwRkJReXhOUVVGTkxFTkJRVU03WVVGRGNrUXNRMEZCUXp0WlFVTkdMRTlCUVU4c1NVRkJTU3hEUVVGRExGbEJRVmtzUTBGQlF5eFZRVUZWTEVOQlFVTXNiMEpCUVc5Q0xFVkJRVVVzVFVGQlRTeERRVUZETEVOQlFVTTdVVUZEY0VVc1EwRkJReXhEUVVGRExFTkJRMGdzUTBGQlF6dEpRVU5LTEVOQlFVTTdTVUZGVFN4UFFVRlBPMUZCUTFvc1NVRkJTU3hKUVVGSkxFTkJRVU1zYVVKQlFXbENMRVZCUVVVN1dVRkRNVUlzVDBGQlR5eERRVUZETEU5QlFVOHNRMEZCUXl4VFFVRlRMRU5CUVVNc1kwRkJZeXhEUVVGRExFbEJRVWtzUTBGQlF5eHBRa0ZCYVVJc1EwRkJReXhEUVVGRE8xTkJRMnhGTzBsQlEwZ3NRMEZCUXp0RFFVTkdJbjA9IiwiaW1wb3J0IHsgUGVuZGluZ1Jlc3BvbnNlIH0gZnJvbSBcIi4uL2xpYi9wZW5kaW5nLXJlc3BvbnNlXCI7XG5pbXBvcnQgeyBhbGxUeXBlcyB9IGZyb20gXCIuL2h0dHAtaW5zdHJ1bWVudFwiO1xuZXhwb3J0IGNsYXNzIERuc0luc3RydW1lbnQge1xuICAgIGRhdGFSZWNlaXZlcjtcbiAgICBvbkNvbXBsZXRlTGlzdGVuZXI7XG4gICAgcGVuZGluZ1Jlc3BvbnNlcyA9IHt9O1xuICAgIGNvbnN0cnVjdG9yKGRhdGFSZWNlaXZlcikge1xuICAgICAgICB0aGlzLmRhdGFSZWNlaXZlciA9IGRhdGFSZWNlaXZlcjtcbiAgICB9XG4gICAgcnVuKGNyYXdsSUQpIHtcbiAgICAgICAgY29uc3QgZmlsdGVyID0geyB1cmxzOiBbXCI8YWxsX3VybHM+XCJdLCB0eXBlczogYWxsVHlwZXMgfTtcbiAgICAgICAgY29uc3QgcmVxdWVzdFN0ZW1zRnJvbUV4dGVuc2lvbiA9IChkZXRhaWxzKSA9PiB7XG4gICAgICAgICAgICByZXR1cm4gKGRldGFpbHMub3JpZ2luVXJsICYmXG4gICAgICAgICAgICAgICAgZGV0YWlscy5vcmlnaW5VcmwuaW5kZXhPZihcIm1vei1leHRlbnNpb246Ly9cIikgPiAtMSAmJlxuICAgICAgICAgICAgICAgIGRldGFpbHMub3JpZ2luVXJsLmluY2x1ZGVzKFwiZmFrZVJlcXVlc3RcIikpO1xuICAgICAgICB9O1xuICAgICAgICAvKlxuICAgICAgICAgKiBBdHRhY2ggaGFuZGxlcnMgdG8gZXZlbnQgbGlzdGVuZXJzXG4gICAgICAgICAqL1xuICAgICAgICB0aGlzLm9uQ29tcGxldGVMaXN0ZW5lciA9IChkZXRhaWxzKSA9PiB7XG4gICAgICAgICAgICAvLyBJZ25vcmUgcmVxdWVzdHMgbWFkZSBieSBleHRlbnNpb25zXG4gICAgICAgICAgICBpZiAocmVxdWVzdFN0ZW1zRnJvbUV4dGVuc2lvbihkZXRhaWxzKSkge1xuICAgICAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGNvbnN0IHBlbmRpbmdSZXNwb25zZSA9IHRoaXMuZ2V0UGVuZGluZ1Jlc3BvbnNlKGRldGFpbHMucmVxdWVzdElkKTtcbiAgICAgICAgICAgIHBlbmRpbmdSZXNwb25zZS5yZXNvbHZlT25Db21wbGV0ZWRFdmVudERldGFpbHMoZGV0YWlscyk7XG4gICAgICAgICAgICB0aGlzLm9uQ29tcGxldGVEbnNIYW5kbGVyKGRldGFpbHMsIGNyYXdsSUQpO1xuICAgICAgICB9O1xuICAgICAgICBicm93c2VyLndlYlJlcXVlc3Qub25Db21wbGV0ZWQuYWRkTGlzdGVuZXIodGhpcy5vbkNvbXBsZXRlTGlzdGVuZXIsIGZpbHRlcik7XG4gICAgfVxuICAgIGNsZWFudXAoKSB7XG4gICAgICAgIGlmICh0aGlzLm9uQ29tcGxldGVMaXN0ZW5lcikge1xuICAgICAgICAgICAgYnJvd3Nlci53ZWJSZXF1ZXN0Lm9uQ29tcGxldGVkLnJlbW92ZUxpc3RlbmVyKHRoaXMub25Db21wbGV0ZUxpc3RlbmVyKTtcbiAgICAgICAgfVxuICAgIH1cbiAgICBnZXRQZW5kaW5nUmVzcG9uc2UocmVxdWVzdElkKSB7XG4gICAgICAgIGlmICghdGhpcy5wZW5kaW5nUmVzcG9uc2VzW3JlcXVlc3RJZF0pIHtcbiAgICAgICAgICAgIHRoaXMucGVuZGluZ1Jlc3BvbnNlc1tyZXF1ZXN0SWRdID0gbmV3IFBlbmRpbmdSZXNwb25zZSgpO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiB0aGlzLnBlbmRpbmdSZXNwb25zZXNbcmVxdWVzdElkXTtcbiAgICB9XG4gICAgaGFuZGxlUmVzb2x2ZWREbnNEYXRhKGRuc1JlY29yZE9iaiwgZGF0YVJlY2VpdmVyKSB7XG4gICAgICAgIC8vIEN1cnJpbmcgdGhlIGRhdGEgcmV0dXJuZWQgYnkgQVBJIGNhbGwuXG4gICAgICAgIHJldHVybiBmdW5jdGlvbiAocmVjb3JkKSB7XG4gICAgICAgICAgICAvLyBHZXQgZGF0YSBmcm9tIEFQSSBjYWxsXG4gICAgICAgICAgICBkbnNSZWNvcmRPYmouYWRkcmVzc2VzID0gcmVjb3JkLmFkZHJlc3Nlcy50b1N0cmluZygpO1xuICAgICAgICAgICAgZG5zUmVjb3JkT2JqLmNhbm9uaWNhbF9uYW1lID0gcmVjb3JkLmNhbm9uaWNhbE5hbWU7XG4gICAgICAgICAgICBkbnNSZWNvcmRPYmouaXNfVFJSID0gcmVjb3JkLmlzVFJSO1xuICAgICAgICAgICAgLy8gU2VuZCBkYXRhIHRvIG1haW4gT3BlbldQTSBkYXRhIGFnZ3JlZ2F0b3IuXG4gICAgICAgICAgICBkYXRhUmVjZWl2ZXIuc2F2ZVJlY29yZChcImRuc19yZXNwb25zZXNcIiwgZG5zUmVjb3JkT2JqKTtcbiAgICAgICAgfTtcbiAgICB9XG4gICAgYXN5bmMgb25Db21wbGV0ZURuc0hhbmRsZXIoZGV0YWlscywgY3Jhd2xJRCkge1xuICAgICAgICAvLyBDcmVhdGUgYW5kIHBvcHVsYXRlIERuc1Jlc29sdmUgb2JqZWN0XG4gICAgICAgIGNvbnN0IGRuc1JlY29yZCA9IHt9O1xuICAgICAgICBkbnNSZWNvcmQuYnJvd3Nlcl9pZCA9IGNyYXdsSUQ7XG4gICAgICAgIGRuc1JlY29yZC5yZXF1ZXN0X2lkID0gTnVtYmVyKGRldGFpbHMucmVxdWVzdElkKTtcbiAgICAgICAgZG5zUmVjb3JkLnVzZWRfYWRkcmVzcyA9IGRldGFpbHMuaXA7XG4gICAgICAgIGNvbnN0IGN1cnJlbnRUaW1lID0gbmV3IERhdGUoZGV0YWlscy50aW1lU3RhbXApO1xuICAgICAgICBkbnNSZWNvcmQudGltZV9zdGFtcCA9IGN1cnJlbnRUaW1lLnRvSVNPU3RyaW5nKCk7XG4gICAgICAgIC8vIFF1ZXJ5IEROUyBBUElcbiAgICAgICAgY29uc3QgdXJsID0gbmV3IFVSTChkZXRhaWxzLnVybCk7XG4gICAgICAgIGRuc1JlY29yZC5ob3N0bmFtZSA9IHVybC5ob3N0bmFtZTtcbiAgICAgICAgY29uc3QgZG5zUmVzb2x2ZSA9IGJyb3dzZXIuZG5zLnJlc29sdmUoZG5zUmVjb3JkLmhvc3RuYW1lLCBbXG4gICAgICAgICAgICBcImNhbm9uaWNhbF9uYW1lXCIsXG4gICAgICAgIF0pO1xuICAgICAgICBkbnNSZXNvbHZlLnRoZW4odGhpcy5oYW5kbGVSZXNvbHZlZERuc0RhdGEoZG5zUmVjb3JkLCB0aGlzLmRhdGFSZWNlaXZlcikpO1xuICAgIH1cbn1cbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaVpHNXpMV2x1YzNSeWRXMWxiblF1YW5NaUxDSnpiM1Z5WTJWU2IyOTBJam9pSWl3aWMyOTFjbU5sY3lJNld5SXVMaTh1TGk4dUxpOXpjbU12WW1GamEyZHliM1Z1WkM5a2JuTXRhVzV6ZEhKMWJXVnVkQzUwY3lKZExDSnVZVzFsY3lJNlcxMHNJbTFoY0hCcGJtZHpJam9pUVVGQlFTeFBRVUZQTEVWQlFVVXNaVUZCWlN4RlFVRkZMRTFCUVUwc2VVSkJRWGxDTEVOQlFVTTdRVUZITVVRc1QwRkJUeXhGUVVGRkxGRkJRVkVzUlVGQlJTeE5RVUZOTEcxQ1FVRnRRaXhEUVVGRE8wRkJSemRETEUxQlFVMHNUMEZCVHl4aFFVRmhPMGxCUTFBc1dVRkJXU3hEUVVGRE8wbEJRM1JDTEd0Q1FVRnJRaXhEUVVGRE8wbEJRMjVDTEdkQ1FVRm5RaXhIUVVWd1FpeEZRVUZGTEVOQlFVTTdTVUZGVUN4WlFVRlpMRmxCUVZrN1VVRkRkRUlzU1VGQlNTeERRVUZETEZsQlFWa3NSMEZCUnl4WlFVRlpMRU5CUVVNN1NVRkRia01zUTBGQlF6dEpRVVZOTEVkQlFVY3NRMEZCUXl4UFFVRlBPMUZCUTJoQ0xFMUJRVTBzVFVGQlRTeEhRVUZyUWl4RlFVRkZMRWxCUVVrc1JVRkJSU3hEUVVGRExGbEJRVmtzUTBGQlF5eEZRVUZGTEV0QlFVc3NSVUZCUlN4UlFVRlJMRVZCUVVVc1EwRkJRenRSUVVWNFJTeE5RVUZOTEhsQ1FVRjVRaXhIUVVGSExFTkJRVU1zVDBGQlR5eEZRVUZGTEVWQlFVVTdXVUZETlVNc1QwRkJUeXhEUVVOTUxFOUJRVThzUTBGQlF5eFRRVUZUTzJkQ1FVTnFRaXhQUVVGUExFTkJRVU1zVTBGQlV5eERRVUZETEU5QlFVOHNRMEZCUXl4clFrRkJhMElzUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXp0blFrRkRiRVFzVDBGQlR5eERRVUZETEZOQlFWTXNRMEZCUXl4UlFVRlJMRU5CUVVNc1lVRkJZU3hEUVVGRExFTkJRekZETEVOQlFVTTdVVUZEU2l4RFFVRkRMRU5CUVVNN1VVRkZSanM3VjBGRlJ6dFJRVU5JTEVsQlFVa3NRMEZCUXl4clFrRkJhMElzUjBGQlJ5eERRVUZETEU5QlFUQkRMRVZCUVVVc1JVRkJSVHRaUVVOMlJTeHhRMEZCY1VNN1dVRkRja01zU1VGQlNTeDVRa0ZCZVVJc1EwRkJReXhQUVVGUExFTkJRVU1zUlVGQlJUdG5Ra0ZEZEVNc1QwRkJUenRoUVVOU08xbEJRMFFzVFVGQlRTeGxRVUZsTEVkQlFVY3NTVUZCU1N4RFFVRkRMR3RDUVVGclFpeERRVUZETEU5QlFVOHNRMEZCUXl4VFFVRlRMRU5CUVVNc1EwRkJRenRaUVVOdVJTeGxRVUZsTEVOQlFVTXNPRUpCUVRoQ0xFTkJRVU1zVDBGQlR5eERRVUZETEVOQlFVTTdXVUZGZUVRc1NVRkJTU3hEUVVGRExHOUNRVUZ2UWl4RFFVRkRMRTlCUVU4c1JVRkJSU3hQUVVGUExFTkJRVU1zUTBGQlF6dFJRVU01UXl4RFFVRkRMRU5CUVVNN1VVRkZSaXhQUVVGUExFTkJRVU1zVlVGQlZTeERRVUZETEZkQlFWY3NRMEZCUXl4WFFVRlhMRU5CUVVNc1NVRkJTU3hEUVVGRExHdENRVUZyUWl4RlFVRkZMRTFCUVUwc1EwRkJReXhEUVVGRE8wbEJRemxGTEVOQlFVTTdTVUZGVFN4UFFVRlBPMUZCUTFvc1NVRkJTU3hKUVVGSkxFTkJRVU1zYTBKQlFXdENMRVZCUVVVN1dVRkRNMElzVDBGQlR5eERRVUZETEZWQlFWVXNRMEZCUXl4WFFVRlhMRU5CUVVNc1kwRkJZeXhEUVVGRExFbEJRVWtzUTBGQlF5eHJRa0ZCYTBJc1EwRkJReXhEUVVGRE8xTkJRM2hGTzBsQlEwZ3NRMEZCUXp0SlFVVlBMR3RDUVVGclFpeERRVUZETEZOQlFWTTdVVUZEYkVNc1NVRkJTU3hEUVVGRExFbEJRVWtzUTBGQlF5eG5Ra0ZCWjBJc1EwRkJReXhUUVVGVExFTkJRVU1zUlVGQlJUdFpRVU55UXl4SlFVRkpMRU5CUVVNc1owSkJRV2RDTEVOQlFVTXNVMEZCVXl4RFFVRkRMRWRCUVVjc1NVRkJTU3hsUVVGbExFVkJRVVVzUTBGQlF6dFRRVU14UkR0UlFVTkVMRTlCUVU4c1NVRkJTU3hEUVVGRExHZENRVUZuUWl4RFFVRkRMRk5CUVZNc1EwRkJReXhEUVVGRE8wbEJRekZETEVOQlFVTTdTVUZGVHl4eFFrRkJjVUlzUTBGQlF5eFpRVUZaTEVWQlFVVXNXVUZCV1R0UlFVTjBSQ3g1UTBGQmVVTTdVVUZEZWtNc1QwRkJUeXhWUVVGVkxFMUJRVTA3V1VGRGNrSXNlVUpCUVhsQ08xbEJRM3BDTEZsQlFWa3NRMEZCUXl4VFFVRlRMRWRCUVVjc1RVRkJUU3hEUVVGRExGTkJRVk1zUTBGQlF5eFJRVUZSTEVWQlFVVXNRMEZCUXp0WlFVTnlSQ3haUVVGWkxFTkJRVU1zWTBGQll5eEhRVUZITEUxQlFVMHNRMEZCUXl4aFFVRmhMRU5CUVVNN1dVRkRia1FzV1VGQldTeERRVUZETEUxQlFVMHNSMEZCUnl4TlFVRk5MRU5CUVVNc1MwRkJTeXhEUVVGRE8xbEJSVzVETERaRFFVRTJRenRaUVVNM1F5eFpRVUZaTEVOQlFVTXNWVUZCVlN4RFFVRkRMR1ZCUVdVc1JVRkJSU3haUVVGWkxFTkJRVU1zUTBGQlF6dFJRVU42UkN4RFFVRkRMRU5CUVVNN1NVRkRTaXhEUVVGRE8wbEJSVThzUzBGQlN5eERRVUZETEc5Q1FVRnZRaXhEUVVOb1F5eFBRVUV3UXl4RlFVTXhReXhQUVVGUE8xRkJSVkFzZDBOQlFYZERPMUZCUTNoRExFMUJRVTBzVTBGQlV5eEhRVUZITEVWQlFXbENMRU5CUVVNN1VVRkRjRU1zVTBGQlV5eERRVUZETEZWQlFWVXNSMEZCUnl4UFFVRlBMRU5CUVVNN1VVRkRMMElzVTBGQlV5eERRVUZETEZWQlFWVXNSMEZCUnl4TlFVRk5MRU5CUVVNc1QwRkJUeXhEUVVGRExGTkJRVk1zUTBGQlF5eERRVUZETzFGQlEycEVMRk5CUVZNc1EwRkJReXhaUVVGWkxFZEJRVWNzVDBGQlR5eERRVUZETEVWQlFVVXNRMEZCUXp0UlFVTndReXhOUVVGTkxGZEJRVmNzUjBGQlJ5eEpRVUZKTEVsQlFVa3NRMEZCUXl4UFFVRlBMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU03VVVGRGFFUXNVMEZCVXl4RFFVRkRMRlZCUVZVc1IwRkJSeXhYUVVGWExFTkJRVU1zVjBGQlZ5eEZRVUZGTEVOQlFVTTdVVUZGYWtRc1owSkJRV2RDTzFGQlEyaENMRTFCUVUwc1IwRkJSeXhIUVVGSExFbEJRVWtzUjBGQlJ5eERRVUZETEU5QlFVOHNRMEZCUXl4SFFVRkhMRU5CUVVNc1EwRkJRenRSUVVOcVF5eFRRVUZUTEVOQlFVTXNVVUZCVVN4SFFVRkhMRWRCUVVjc1EwRkJReXhSUVVGUkxFTkJRVU03VVVGRGJFTXNUVUZCVFN4VlFVRlZMRWRCUVVjc1QwRkJUeXhEUVVGRExFZEJRVWNzUTBGQlF5eFBRVUZQTEVOQlFVTXNVMEZCVXl4RFFVRkRMRkZCUVZFc1JVRkJSVHRaUVVONlJDeG5Ra0ZCWjBJN1UwRkRha0lzUTBGQlF5eERRVUZETzFGQlEwZ3NWVUZCVlN4RFFVRkRMRWxCUVVrc1EwRkJReXhKUVVGSkxFTkJRVU1zY1VKQlFYRkNMRU5CUVVNc1UwRkJVeXhGUVVGRkxFbEJRVWtzUTBGQlF5eFpRVUZaTEVOQlFVTXNRMEZCUXl4RFFVRkRPMGxCUXpWRkxFTkJRVU03UTBGRFJpSjkiLCJpbXBvcnQgeyBpbmNyZW1lbnRlZEV2ZW50T3JkaW5hbCB9IGZyb20gXCIuLi9saWIvZXh0ZW5zaW9uLXNlc3Npb24tZXZlbnQtb3JkaW5hbFwiO1xuaW1wb3J0IHsgZXh0ZW5zaW9uU2Vzc2lvblV1aWQgfSBmcm9tIFwiLi4vbGliL2V4dGVuc2lvbi1zZXNzaW9uLXV1aWRcIjtcbmltcG9ydCB7IEh0dHBQb3N0UGFyc2VyIH0gZnJvbSBcIi4uL2xpYi9odHRwLXBvc3QtcGFyc2VyXCI7XG5pbXBvcnQgeyBQZW5kaW5nUmVxdWVzdCB9IGZyb20gXCIuLi9saWIvcGVuZGluZy1yZXF1ZXN0XCI7XG5pbXBvcnQgeyBQZW5kaW5nUmVzcG9uc2UgfSBmcm9tIFwiLi4vbGliL3BlbmRpbmctcmVzcG9uc2VcIjtcbmltcG9ydCB7IGJvb2xUb0ludCwgZXNjYXBlU3RyaW5nLCBlc2NhcGVVcmwgfSBmcm9tIFwiLi4vbGliL3N0cmluZy11dGlsc1wiO1xuLyoqXG4gKiBOb3RlOiBEaWZmZXJlbnQgcGFydHMgb2YgdGhlIGRlc2lyZWQgaW5mb3JtYXRpb24gYXJyaXZlcyBpbiBkaWZmZXJlbnQgZXZlbnRzIGFzIHBlciBiZWxvdzpcbiAqIHJlcXVlc3QgPSBoZWFkZXJzIGluIG9uQmVmb3JlU2VuZEhlYWRlcnMgKyBib2R5IGluIG9uQmVmb3JlUmVxdWVzdFxuICogcmVzcG9uc2UgPSBoZWFkZXJzIGluIG9uQ29tcGxldGVkICsgYm9keSB2aWEgYSBvbkJlZm9yZVJlcXVlc3QgZmlsdGVyXG4gKiByZWRpcmVjdCA9IG9yaWdpbmFsIHJlcXVlc3QgaGVhZGVycytib2R5LCBmb2xsb3dlZCBieSBhIG9uQmVmb3JlUmVkaXJlY3QgYW5kIHRoZW4gYSBuZXcgc2V0IG9mIHJlcXVlc3QgaGVhZGVycytib2R5IGFuZCByZXNwb25zZSBoZWFkZXJzK2JvZHlcbiAqIERvY3M6IGh0dHBzOi8vZGV2ZWxvcGVyLm1vemlsbGEub3JnL2VuLVVTL2RvY3MvVXNlcjp3YmFtYmVyZy93ZWJSZXF1ZXN0LlJlcXVlc3REZXRhaWxzXG4gKi9cbmNvbnN0IGFsbFR5cGVzID0gW1xuICAgIFwiYmVhY29uXCIsXG4gICAgXCJjc3BfcmVwb3J0XCIsXG4gICAgXCJmb250XCIsXG4gICAgXCJpbWFnZVwiLFxuICAgIFwiaW1hZ2VzZXRcIixcbiAgICBcIm1haW5fZnJhbWVcIixcbiAgICBcIm1lZGlhXCIsXG4gICAgXCJvYmplY3RcIixcbiAgICBcIm9iamVjdF9zdWJyZXF1ZXN0XCIsXG4gICAgXCJwaW5nXCIsXG4gICAgXCJzY3JpcHRcIixcbiAgICBcInNwZWN1bGF0aXZlXCIsXG4gICAgXCJzdHlsZXNoZWV0XCIsXG4gICAgXCJzdWJfZnJhbWVcIixcbiAgICBcIndlYl9tYW5pZmVzdFwiLFxuICAgIFwid2Vic29ja2V0XCIsXG4gICAgXCJ4bWxfZHRkXCIsXG4gICAgXCJ4bWxodHRwcmVxdWVzdFwiLFxuICAgIFwieHNsdFwiLFxuICAgIFwib3RoZXJcIixcbl07XG5leHBvcnQgeyBhbGxUeXBlcyB9O1xuZXhwb3J0IGNsYXNzIEh0dHBJbnN0cnVtZW50IHtcbiAgICBkYXRhUmVjZWl2ZXI7XG4gICAgcGVuZGluZ1JlcXVlc3RzID0ge307XG4gICAgcGVuZGluZ1Jlc3BvbnNlcyA9IHt9O1xuICAgIG9uQmVmb3JlUmVxdWVzdExpc3RlbmVyO1xuICAgIG9uQmVmb3JlU2VuZEhlYWRlcnNMaXN0ZW5lcjtcbiAgICBvbkJlZm9yZVJlZGlyZWN0TGlzdGVuZXI7XG4gICAgb25Db21wbGV0ZWRMaXN0ZW5lcjtcbiAgICBjb25zdHJ1Y3RvcihkYXRhUmVjZWl2ZXIpIHtcbiAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIgPSBkYXRhUmVjZWl2ZXI7XG4gICAgfVxuICAgIHJ1bihjcmF3bElELCBzYXZlQ29udGVudE9wdGlvbikge1xuICAgICAgICBjb25zdCBmaWx0ZXIgPSB7IHVybHM6IFtcIjxhbGxfdXJscz5cIl0sIHR5cGVzOiBhbGxUeXBlcyB9O1xuICAgICAgICBjb25zdCByZXF1ZXN0U3RlbXNGcm9tRXh0ZW5zaW9uID0gKGRldGFpbHMpID0+IHtcbiAgICAgICAgICAgIHJldHVybiAoZGV0YWlscy5vcmlnaW5VcmwgJiYgZGV0YWlscy5vcmlnaW5VcmwuaW5kZXhPZihcIm1vei1leHRlbnNpb246Ly9cIikgPiAtMSk7XG4gICAgICAgIH07XG4gICAgICAgIC8qXG4gICAgICAgICAqIEF0dGFjaCBoYW5kbGVycyB0byBldmVudCBsaXN0ZW5lcnNcbiAgICAgICAgICovXG4gICAgICAgIHRoaXMub25CZWZvcmVSZXF1ZXN0TGlzdGVuZXIgPSAoZGV0YWlscykgPT4ge1xuICAgICAgICAgICAgY29uc3QgYmxvY2tpbmdSZXNwb25zZVRoYXREb2VzTm90aGluZyA9IHt9O1xuICAgICAgICAgICAgLy8gSWdub3JlIHJlcXVlc3RzIG1hZGUgYnkgZXh0ZW5zaW9uc1xuICAgICAgICAgICAgaWYgKHJlcXVlc3RTdGVtc0Zyb21FeHRlbnNpb24oZGV0YWlscykpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gYmxvY2tpbmdSZXNwb25zZVRoYXREb2VzTm90aGluZztcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGNvbnN0IHBlbmRpbmdSZXF1ZXN0ID0gdGhpcy5nZXRQZW5kaW5nUmVxdWVzdChkZXRhaWxzLnJlcXVlc3RJZCk7XG4gICAgICAgICAgICBwZW5kaW5nUmVxdWVzdC5yZXNvbHZlT25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzKGRldGFpbHMpO1xuICAgICAgICAgICAgY29uc3QgcGVuZGluZ1Jlc3BvbnNlID0gdGhpcy5nZXRQZW5kaW5nUmVzcG9uc2UoZGV0YWlscy5yZXF1ZXN0SWQpO1xuICAgICAgICAgICAgcGVuZGluZ1Jlc3BvbnNlLnJlc29sdmVPbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHMoZGV0YWlscyk7XG4gICAgICAgICAgICBpZiAodGhpcy5zaG91bGRTYXZlQ29udGVudChzYXZlQ29udGVudE9wdGlvbiwgZGV0YWlscy50eXBlKSkge1xuICAgICAgICAgICAgICAgIHBlbmRpbmdSZXNwb25zZS5hZGRSZXNwb25zZVJlc3BvbnNlQm9keUxpc3RlbmVyKGRldGFpbHMpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgcmV0dXJuIGJsb2NraW5nUmVzcG9uc2VUaGF0RG9lc05vdGhpbmc7XG4gICAgICAgIH07XG4gICAgICAgIGJyb3dzZXIud2ViUmVxdWVzdC5vbkJlZm9yZVJlcXVlc3QuYWRkTGlzdGVuZXIodGhpcy5vbkJlZm9yZVJlcXVlc3RMaXN0ZW5lciwgZmlsdGVyLCB0aGlzLmlzQ29udGVudFNhdmluZ0VuYWJsZWQoc2F2ZUNvbnRlbnRPcHRpb24pXG4gICAgICAgICAgICA/IFtcInJlcXVlc3RCb2R5XCIsIFwiYmxvY2tpbmdcIl1cbiAgICAgICAgICAgIDogW1wicmVxdWVzdEJvZHlcIl0pO1xuICAgICAgICB0aGlzLm9uQmVmb3JlU2VuZEhlYWRlcnNMaXN0ZW5lciA9IChkZXRhaWxzKSA9PiB7XG4gICAgICAgICAgICAvLyBJZ25vcmUgcmVxdWVzdHMgbWFkZSBieSBleHRlbnNpb25zXG4gICAgICAgICAgICBpZiAocmVxdWVzdFN0ZW1zRnJvbUV4dGVuc2lvbihkZXRhaWxzKSkge1xuICAgICAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGNvbnN0IHBlbmRpbmdSZXF1ZXN0ID0gdGhpcy5nZXRQZW5kaW5nUmVxdWVzdChkZXRhaWxzLnJlcXVlc3RJZCk7XG4gICAgICAgICAgICBwZW5kaW5nUmVxdWVzdC5yZXNvbHZlT25CZWZvcmVTZW5kSGVhZGVyc0V2ZW50RGV0YWlscyhkZXRhaWxzKTtcbiAgICAgICAgICAgIHRoaXMub25CZWZvcmVTZW5kSGVhZGVyc0hhbmRsZXIoZGV0YWlscywgY3Jhd2xJRCwgaW5jcmVtZW50ZWRFdmVudE9yZGluYWwoKSk7XG4gICAgICAgIH07XG4gICAgICAgIGJyb3dzZXIud2ViUmVxdWVzdC5vbkJlZm9yZVNlbmRIZWFkZXJzLmFkZExpc3RlbmVyKHRoaXMub25CZWZvcmVTZW5kSGVhZGVyc0xpc3RlbmVyLCBmaWx0ZXIsIFtcInJlcXVlc3RIZWFkZXJzXCJdKTtcbiAgICAgICAgdGhpcy5vbkJlZm9yZVJlZGlyZWN0TGlzdGVuZXIgPSAoZGV0YWlscykgPT4ge1xuICAgICAgICAgICAgLy8gSWdub3JlIHJlcXVlc3RzIG1hZGUgYnkgZXh0ZW5zaW9uc1xuICAgICAgICAgICAgaWYgKHJlcXVlc3RTdGVtc0Zyb21FeHRlbnNpb24oZGV0YWlscykpIHtcbiAgICAgICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICB0aGlzLm9uQmVmb3JlUmVkaXJlY3RIYW5kbGVyKGRldGFpbHMsIGNyYXdsSUQsIGluY3JlbWVudGVkRXZlbnRPcmRpbmFsKCkpO1xuICAgICAgICB9O1xuICAgICAgICBicm93c2VyLndlYlJlcXVlc3Qub25CZWZvcmVSZWRpcmVjdC5hZGRMaXN0ZW5lcih0aGlzLm9uQmVmb3JlUmVkaXJlY3RMaXN0ZW5lciwgZmlsdGVyLCBbXCJyZXNwb25zZUhlYWRlcnNcIl0pO1xuICAgICAgICB0aGlzLm9uQ29tcGxldGVkTGlzdGVuZXIgPSAoZGV0YWlscykgPT4ge1xuICAgICAgICAgICAgLy8gSWdub3JlIHJlcXVlc3RzIG1hZGUgYnkgZXh0ZW5zaW9uc1xuICAgICAgICAgICAgaWYgKHJlcXVlc3RTdGVtc0Zyb21FeHRlbnNpb24oZGV0YWlscykpIHtcbiAgICAgICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBjb25zdCBwZW5kaW5nUmVzcG9uc2UgPSB0aGlzLmdldFBlbmRpbmdSZXNwb25zZShkZXRhaWxzLnJlcXVlc3RJZCk7XG4gICAgICAgICAgICBwZW5kaW5nUmVzcG9uc2UucmVzb2x2ZU9uQ29tcGxldGVkRXZlbnREZXRhaWxzKGRldGFpbHMpO1xuICAgICAgICAgICAgdGhpcy5vbkNvbXBsZXRlZEhhbmRsZXIoZGV0YWlscywgY3Jhd2xJRCwgaW5jcmVtZW50ZWRFdmVudE9yZGluYWwoKSwgc2F2ZUNvbnRlbnRPcHRpb24pO1xuICAgICAgICB9O1xuICAgICAgICBicm93c2VyLndlYlJlcXVlc3Qub25Db21wbGV0ZWQuYWRkTGlzdGVuZXIodGhpcy5vbkNvbXBsZXRlZExpc3RlbmVyLCBmaWx0ZXIsIFtcInJlc3BvbnNlSGVhZGVyc1wiXSk7XG4gICAgfVxuICAgIGNsZWFudXAoKSB7XG4gICAgICAgIGlmICh0aGlzLm9uQmVmb3JlUmVxdWVzdExpc3RlbmVyKSB7XG4gICAgICAgICAgICBicm93c2VyLndlYlJlcXVlc3Qub25CZWZvcmVSZXF1ZXN0LnJlbW92ZUxpc3RlbmVyKHRoaXMub25CZWZvcmVSZXF1ZXN0TGlzdGVuZXIpO1xuICAgICAgICB9XG4gICAgICAgIGlmICh0aGlzLm9uQmVmb3JlU2VuZEhlYWRlcnNMaXN0ZW5lcikge1xuICAgICAgICAgICAgYnJvd3Nlci53ZWJSZXF1ZXN0Lm9uQmVmb3JlU2VuZEhlYWRlcnMucmVtb3ZlTGlzdGVuZXIodGhpcy5vbkJlZm9yZVNlbmRIZWFkZXJzTGlzdGVuZXIpO1xuICAgICAgICB9XG4gICAgICAgIGlmICh0aGlzLm9uQmVmb3JlUmVkaXJlY3RMaXN0ZW5lcikge1xuICAgICAgICAgICAgYnJvd3Nlci53ZWJSZXF1ZXN0Lm9uQmVmb3JlUmVkaXJlY3QucmVtb3ZlTGlzdGVuZXIodGhpcy5vbkJlZm9yZVJlZGlyZWN0TGlzdGVuZXIpO1xuICAgICAgICB9XG4gICAgICAgIGlmICh0aGlzLm9uQ29tcGxldGVkTGlzdGVuZXIpIHtcbiAgICAgICAgICAgIGJyb3dzZXIud2ViUmVxdWVzdC5vbkNvbXBsZXRlZC5yZW1vdmVMaXN0ZW5lcih0aGlzLm9uQ29tcGxldGVkTGlzdGVuZXIpO1xuICAgICAgICB9XG4gICAgfVxuICAgIGlzQ29udGVudFNhdmluZ0VuYWJsZWQoc2F2ZUNvbnRlbnRPcHRpb24pIHtcbiAgICAgICAgaWYgKHNhdmVDb250ZW50T3B0aW9uID09PSB0cnVlKSB7XG4gICAgICAgICAgICByZXR1cm4gdHJ1ZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoc2F2ZUNvbnRlbnRPcHRpb24gPT09IGZhbHNlKSB7XG4gICAgICAgICAgICByZXR1cm4gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHRoaXMuc2F2ZUNvbnRlbnRSZXNvdXJjZVR5cGVzKHNhdmVDb250ZW50T3B0aW9uKS5sZW5ndGggPiAwO1xuICAgIH1cbiAgICBzYXZlQ29udGVudFJlc291cmNlVHlwZXMoc2F2ZUNvbnRlbnRPcHRpb24pIHtcbiAgICAgICAgcmV0dXJuIHNhdmVDb250ZW50T3B0aW9uLnNwbGl0KFwiLFwiKTtcbiAgICB9XG4gICAgLyoqXG4gICAgICogV2UgcmVseSBvbiB0aGUgcmVzb3VyY2UgdHlwZSB0byBmaWx0ZXIgcmVzcG9uc2VzXG4gICAgICogU2VlOiBodHRwczovL2RldmVsb3Blci5tb3ppbGxhLm9yZy9lbi1VUy9kb2NzL01vemlsbGEvQWRkLW9ucy9XZWJFeHRlbnNpb25zL0FQSS93ZWJSZXF1ZXN0L1Jlc291cmNlVHlwZVxuICAgICAqXG4gICAgICogQHBhcmFtIHNhdmVDb250ZW50T3B0aW9uXG4gICAgICogQHBhcmFtIHJlc291cmNlVHlwZVxuICAgICAqL1xuICAgIHNob3VsZFNhdmVDb250ZW50KHNhdmVDb250ZW50T3B0aW9uLCByZXNvdXJjZVR5cGUpIHtcbiAgICAgICAgaWYgKHNhdmVDb250ZW50T3B0aW9uID09PSB0cnVlKSB7XG4gICAgICAgICAgICByZXR1cm4gdHJ1ZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoc2F2ZUNvbnRlbnRPcHRpb24gPT09IGZhbHNlKSB7XG4gICAgICAgICAgICByZXR1cm4gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHRoaXMuc2F2ZUNvbnRlbnRSZXNvdXJjZVR5cGVzKHNhdmVDb250ZW50T3B0aW9uKS5pbmNsdWRlcyhyZXNvdXJjZVR5cGUpO1xuICAgIH1cbiAgICBnZXRQZW5kaW5nUmVxdWVzdChyZXF1ZXN0SWQpIHtcbiAgICAgICAgaWYgKCF0aGlzLnBlbmRpbmdSZXF1ZXN0c1tyZXF1ZXN0SWRdKSB7XG4gICAgICAgICAgICB0aGlzLnBlbmRpbmdSZXF1ZXN0c1tyZXF1ZXN0SWRdID0gbmV3IFBlbmRpbmdSZXF1ZXN0KCk7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHRoaXMucGVuZGluZ1JlcXVlc3RzW3JlcXVlc3RJZF07XG4gICAgfVxuICAgIGdldFBlbmRpbmdSZXNwb25zZShyZXF1ZXN0SWQpIHtcbiAgICAgICAgaWYgKCF0aGlzLnBlbmRpbmdSZXNwb25zZXNbcmVxdWVzdElkXSkge1xuICAgICAgICAgICAgdGhpcy5wZW5kaW5nUmVzcG9uc2VzW3JlcXVlc3RJZF0gPSBuZXcgUGVuZGluZ1Jlc3BvbnNlKCk7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHRoaXMucGVuZGluZ1Jlc3BvbnNlc1tyZXF1ZXN0SWRdO1xuICAgIH1cbiAgICAvKlxuICAgICAqIEhUVFAgUmVxdWVzdCBIYW5kbGVyIGFuZCBIZWxwZXIgRnVuY3Rpb25zXG4gICAgICovXG4gICAgYXN5bmMgb25CZWZvcmVTZW5kSGVhZGVyc0hhbmRsZXIoZGV0YWlscywgY3Jhd2xJRCwgZXZlbnRPcmRpbmFsKSB7XG4gICAgICAgIGNvbnN0IHRhYiA9IGRldGFpbHMudGFiSWQgPiAtMVxuICAgICAgICAgICAgPyBhd2FpdCBicm93c2VyLnRhYnMuZ2V0KGRldGFpbHMudGFiSWQpXG4gICAgICAgICAgICA6IHsgd2luZG93SWQ6IHVuZGVmaW5lZCwgaW5jb2duaXRvOiB1bmRlZmluZWQsIHVybDogdW5kZWZpbmVkIH07XG4gICAgICAgIGNvbnN0IHVwZGF0ZSA9IHt9O1xuICAgICAgICB1cGRhdGUuaW5jb2duaXRvID0gYm9vbFRvSW50KHRhYi5pbmNvZ25pdG8pO1xuICAgICAgICB1cGRhdGUuYnJvd3Nlcl9pZCA9IGNyYXdsSUQ7XG4gICAgICAgIHVwZGF0ZS5leHRlbnNpb25fc2Vzc2lvbl91dWlkID0gZXh0ZW5zaW9uU2Vzc2lvblV1aWQ7XG4gICAgICAgIHVwZGF0ZS5ldmVudF9vcmRpbmFsID0gZXZlbnRPcmRpbmFsO1xuICAgICAgICB1cGRhdGUud2luZG93X2lkID0gdGFiLndpbmRvd0lkO1xuICAgICAgICB1cGRhdGUudGFiX2lkID0gZGV0YWlscy50YWJJZDtcbiAgICAgICAgdXBkYXRlLmZyYW1lX2lkID0gZGV0YWlscy5mcmFtZUlkO1xuICAgICAgICAvLyByZXF1ZXN0SWQgaXMgYSB1bmlxdWUgaWRlbnRpZmllciB0aGF0IGNhbiBiZSB1c2VkIHRvIGxpbmsgcmVxdWVzdHMgYW5kIHJlc3BvbnNlc1xuICAgICAgICB1cGRhdGUucmVxdWVzdF9pZCA9IE51bWJlcihkZXRhaWxzLnJlcXVlc3RJZCk7XG4gICAgICAgIGNvbnN0IHVybCA9IGRldGFpbHMudXJsO1xuICAgICAgICB1cGRhdGUudXJsID0gZXNjYXBlVXJsKHVybCk7XG4gICAgICAgIGNvbnN0IHJlcXVlc3RNZXRob2QgPSBkZXRhaWxzLm1ldGhvZDtcbiAgICAgICAgdXBkYXRlLm1ldGhvZCA9IGVzY2FwZVN0cmluZyhyZXF1ZXN0TWV0aG9kKTtcbiAgICAgICAgY29uc3QgY3VycmVudF90aW1lID0gbmV3IERhdGUoZGV0YWlscy50aW1lU3RhbXApO1xuICAgICAgICB1cGRhdGUudGltZV9zdGFtcCA9IGN1cnJlbnRfdGltZS50b0lTT1N0cmluZygpO1xuICAgICAgICBsZXQgZW5jb2RpbmdUeXBlID0gXCJcIjtcbiAgICAgICAgbGV0IHJlZmVycmVyID0gXCJcIjtcbiAgICAgICAgY29uc3QgaGVhZGVycyA9IFtdO1xuICAgICAgICBsZXQgaXNPY3NwID0gZmFsc2U7XG4gICAgICAgIGlmIChkZXRhaWxzLnJlcXVlc3RIZWFkZXJzKSB7XG4gICAgICAgICAgICBkZXRhaWxzLnJlcXVlc3RIZWFkZXJzLm1hcCgocmVxdWVzdEhlYWRlcikgPT4ge1xuICAgICAgICAgICAgICAgIGNvbnN0IHsgbmFtZSwgdmFsdWUgfSA9IHJlcXVlc3RIZWFkZXI7XG4gICAgICAgICAgICAgICAgY29uc3QgaGVhZGVyX3BhaXIgPSBbXTtcbiAgICAgICAgICAgICAgICBoZWFkZXJfcGFpci5wdXNoKGVzY2FwZVN0cmluZyhuYW1lKSk7XG4gICAgICAgICAgICAgICAgaGVhZGVyX3BhaXIucHVzaChlc2NhcGVTdHJpbmcodmFsdWUpKTtcbiAgICAgICAgICAgICAgICBoZWFkZXJzLnB1c2goaGVhZGVyX3BhaXIpO1xuICAgICAgICAgICAgICAgIGlmIChuYW1lID09PSBcIkNvbnRlbnQtVHlwZVwiKSB7XG4gICAgICAgICAgICAgICAgICAgIGVuY29kaW5nVHlwZSA9IHZhbHVlO1xuICAgICAgICAgICAgICAgICAgICBpZiAoZW5jb2RpbmdUeXBlLmluZGV4T2YoXCJhcHBsaWNhdGlvbi9vY3NwLXJlcXVlc3RcIikgIT09IC0xKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBpc09jc3AgPSB0cnVlO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIGlmIChuYW1lID09PSBcIlJlZmVyZXJcIikge1xuICAgICAgICAgICAgICAgICAgICByZWZlcnJlciA9IHZhbHVlO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIHVwZGF0ZS5yZWZlcnJlciA9IGVzY2FwZVN0cmluZyhyZWZlcnJlcik7XG4gICAgICAgIGlmIChyZXF1ZXN0TWV0aG9kID09PSBcIlBPU1RcIiAmJiAhaXNPY3NwIC8qIGRvbid0IHByb2Nlc3MgT0NTUCByZXF1ZXN0cyAqLykge1xuICAgICAgICAgICAgY29uc3QgcGVuZGluZ1JlcXVlc3QgPSB0aGlzLmdldFBlbmRpbmdSZXF1ZXN0KGRldGFpbHMucmVxdWVzdElkKTtcbiAgICAgICAgICAgIGNvbnN0IHJlc29sdmVkID0gYXdhaXQgcGVuZGluZ1JlcXVlc3QucmVzb2x2ZWRXaXRoaW5UaW1lb3V0KDEwMDApO1xuICAgICAgICAgICAgaWYgKCFyZXNvbHZlZCkge1xuICAgICAgICAgICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLmxvZ0Vycm9yKFwiUGVuZGluZyByZXF1ZXN0IHRpbWVkIG91dCB3YWl0aW5nIGZvciBkYXRhIGZyb20gYm90aCBvbkJlZm9yZVJlcXVlc3QgYW5kIG9uQmVmb3JlU2VuZEhlYWRlcnMgZXZlbnRzXCIpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgY29uc3Qgb25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzID0gYXdhaXQgcGVuZGluZ1JlcXVlc3Qub25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzO1xuICAgICAgICAgICAgICAgIGNvbnN0IHJlcXVlc3RCb2R5ID0gb25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzLnJlcXVlc3RCb2R5O1xuICAgICAgICAgICAgICAgIGlmIChyZXF1ZXN0Qm9keSkge1xuICAgICAgICAgICAgICAgICAgICBjb25zdCBwb3N0UGFyc2VyID0gbmV3IEh0dHBQb3N0UGFyc2VyKG9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscywgdGhpcy5kYXRhUmVjZWl2ZXIpO1xuICAgICAgICAgICAgICAgICAgICBjb25zdCBwb3N0T2JqID0gcG9zdFBhcnNlci5wYXJzZVBvc3RSZXF1ZXN0KCk7XG4gICAgICAgICAgICAgICAgICAgIC8vIEFkZCAoUE9TVCkgcmVxdWVzdCBoZWFkZXJzIGZyb20gdXBsb2FkIHN0cmVhbVxuICAgICAgICAgICAgICAgICAgICBpZiAoXCJwb3N0X2hlYWRlcnNcIiBpbiBwb3N0T2JqKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAvLyBPbmx5IHN0b3JlIFBPU1QgaGVhZGVycyB0aGF0IHdlIGtub3cgYW5kIG5lZWQuIFdlIG1heSBtaXNpbnRlcnByZXQgUE9TVCBkYXRhIGFzIGhlYWRlcnNcbiAgICAgICAgICAgICAgICAgICAgICAgIC8vIGFzIGRldGVjdGlvbiBpcyBiYXNlZCBvbiBcImtleTp2YWx1ZVwiIGZvcm1hdCAobm9uLWhlYWRlciBQT1NUIGRhdGEgY2FuIGJlIGluIHRoaXMgZm9ybWF0IGFzIHdlbGwpXG4gICAgICAgICAgICAgICAgICAgICAgICBjb25zdCBjb250ZW50SGVhZGVycyA9IFtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBcIkNvbnRlbnQtVHlwZVwiLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIFwiQ29udGVudC1EaXNwb3NpdGlvblwiLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIFwiQ29udGVudC1MZW5ndGhcIixcbiAgICAgICAgICAgICAgICAgICAgICAgIF07XG4gICAgICAgICAgICAgICAgICAgICAgICBmb3IgKGNvbnN0IG5hbWUgaW4gcG9zdE9iai5wb3N0X2hlYWRlcnMpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBpZiAoY29udGVudEhlYWRlcnMuaW5jbHVkZXMobmFtZSkpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgY29uc3QgaGVhZGVyX3BhaXIgPSBbXTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaGVhZGVyX3BhaXIucHVzaChlc2NhcGVTdHJpbmcobmFtZSkpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBoZWFkZXJfcGFpci5wdXNoKGVzY2FwZVN0cmluZyhwb3N0T2JqLnBvc3RfaGVhZGVyc1tuYW1lXSkpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBoZWFkZXJzLnB1c2goaGVhZGVyX3BhaXIpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAvLyB3ZSBzdG9yZSBQT1NUIGJvZHkgaW4gSlNPTiBmb3JtYXQsIGV4Y2VwdCB3aGVuIGl0J3MgYSBzdHJpbmcgd2l0aG91dCBhIChrZXktdmFsdWUpIHN0cnVjdHVyZVxuICAgICAgICAgICAgICAgICAgICBpZiAoXCJwb3N0X2JvZHlcIiBpbiBwb3N0T2JqKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICB1cGRhdGUucG9zdF9ib2R5ID0gcG9zdE9iai5wb3N0X2JvZHk7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKFwicG9zdF9ib2R5X3Jhd1wiIGluIHBvc3RPYmopIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHVwZGF0ZS5wb3N0X2JvZHlfcmF3ID0gcG9zdE9iai5wb3N0X2JvZHlfcmF3O1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIHVwZGF0ZS5oZWFkZXJzID0gSlNPTi5zdHJpbmdpZnkoaGVhZGVycyk7XG4gICAgICAgIC8vIENoZWNrIGlmIHhoclxuICAgICAgICBjb25zdCBpc1hIUiA9IGRldGFpbHMudHlwZSA9PT0gXCJ4bWxodHRwcmVxdWVzdFwiO1xuICAgICAgICB1cGRhdGUuaXNfWEhSID0gYm9vbFRvSW50KGlzWEhSKTtcbiAgICAgICAgLy8gR3JhYiB0aGUgdHJpZ2dlcmluZyBhbmQgbG9hZGluZyBQcmluY2lwYWxzXG4gICAgICAgIGxldCB0cmlnZ2VyaW5nT3JpZ2luO1xuICAgICAgICBsZXQgbG9hZGluZ09yaWdpbjtcbiAgICAgICAgaWYgKGRldGFpbHMub3JpZ2luVXJsKSB7XG4gICAgICAgICAgICBjb25zdCBwYXJzZWRPcmlnaW5VcmwgPSBuZXcgVVJMKGRldGFpbHMub3JpZ2luVXJsKTtcbiAgICAgICAgICAgIHRyaWdnZXJpbmdPcmlnaW4gPSBwYXJzZWRPcmlnaW5Vcmwub3JpZ2luO1xuICAgICAgICB9XG4gICAgICAgIGlmIChkZXRhaWxzLmRvY3VtZW50VXJsKSB7XG4gICAgICAgICAgICBjb25zdCBwYXJzZWREb2N1bWVudFVybCA9IG5ldyBVUkwoZGV0YWlscy5kb2N1bWVudFVybCk7XG4gICAgICAgICAgICBsb2FkaW5nT3JpZ2luID0gcGFyc2VkRG9jdW1lbnRVcmwub3JpZ2luO1xuICAgICAgICB9XG4gICAgICAgIHVwZGF0ZS50cmlnZ2VyaW5nX29yaWdpbiA9IGVzY2FwZVN0cmluZyh0cmlnZ2VyaW5nT3JpZ2luKTtcbiAgICAgICAgdXBkYXRlLmxvYWRpbmdfb3JpZ2luID0gZXNjYXBlU3RyaW5nKGxvYWRpbmdPcmlnaW4pO1xuICAgICAgICAvLyBsb2FkaW5nRG9jdW1lbnQncyBocmVmXG4gICAgICAgIC8vIFRoZSBsb2FkaW5nRG9jdW1lbnQgaXMgdGhlIGRvY3VtZW50IHRoZSBlbGVtZW50IHJlc2lkZXMsIHJlZ2FyZGxlc3Mgb2ZcbiAgICAgICAgLy8gaG93IHRoZSBsb2FkIHdhcyB0cmlnZ2VyZWQuXG4gICAgICAgIGNvbnN0IGxvYWRpbmdIcmVmID0gZGV0YWlscy5kb2N1bWVudFVybDtcbiAgICAgICAgdXBkYXRlLmxvYWRpbmdfaHJlZiA9IGVzY2FwZVN0cmluZyhsb2FkaW5nSHJlZik7XG4gICAgICAgIC8vIHJlc291cmNlVHlwZSBvZiB0aGUgcmVxdWVzdGluZyBub2RlLiBUaGlzIGlzIHNldCBieSB0aGUgdHlwZSBvZlxuICAgICAgICAvLyBub2RlIG1ha2luZyB0aGUgcmVxdWVzdCAoaS5lLiBhbiA8aW1nIHNyYz0uLi4+IG5vZGUgd2lsbCBzZXQgdG8gdHlwZSBcImltYWdlXCIpLlxuICAgICAgICAvLyBEb2N1bWVudGF0aW9uOlxuICAgICAgICAvLyBodHRwczovL2RldmVsb3Blci5tb3ppbGxhLm9yZy9lbi1VUy9kb2NzL01vemlsbGEvQWRkLW9ucy9XZWJFeHRlbnNpb25zL0FQSS93ZWJSZXF1ZXN0L1Jlc291cmNlVHlwZVxuICAgICAgICB1cGRhdGUucmVzb3VyY2VfdHlwZSA9IGRldGFpbHMudHlwZTtcbiAgICAgICAgLypcbiAgICAgICAgLy8gVE9ETzogUmVmYWN0b3IgdG8gY29ycmVzcG9uZGluZyB3ZWJleHQgbG9naWMgb3IgZGlzY2FyZFxuICAgICAgICBjb25zdCBUaGlyZFBhcnR5VXRpbCA9IENjW1wiQG1vemlsbGEub3JnL3RoaXJkcGFydHl1dGlsOzFcIl0uZ2V0U2VydmljZShcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBDaS5tb3pJVGhpcmRQYXJ0eVV0aWwpO1xuICAgICAgICAvLyBEbyB0aGlyZC1wYXJ0eSBjaGVja3NcbiAgICAgICAgLy8gVGhlc2Ugc3BlY2lmaWMgY2hlY2tzIGFyZSBkb25lIGJlY2F1c2UgaXQncyB3aGF0J3MgdXNlZCBpbiBUcmFja2luZyBQcm90ZWN0aW9uXG4gICAgICAgIC8vIFNlZTogaHR0cDovL3NlYXJjaGZveC5vcmcvbW96aWxsYS1jZW50cmFsL3NvdXJjZS9uZXR3ZXJrL2Jhc2UvbnNDaGFubmVsQ2xhc3NpZmllci5jcHAjMTA3XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgY29uc3QgaXNUaGlyZFBhcnR5Q2hhbm5lbCA9IFRoaXJkUGFydHlVdGlsLmlzVGhpcmRQYXJ0eUNoYW5uZWwoZGV0YWlscyk7XG4gICAgICAgICAgY29uc3QgdG9wV2luZG93ID0gVGhpcmRQYXJ0eVV0aWwuZ2V0VG9wV2luZG93Rm9yQ2hhbm5lbChkZXRhaWxzKTtcbiAgICAgICAgICBjb25zdCB0b3BVUkkgPSBUaGlyZFBhcnR5VXRpbC5nZXRVUklGcm9tV2luZG93KHRvcFdpbmRvdyk7XG4gICAgICAgICAgaWYgKHRvcFVSSSkge1xuICAgICAgICAgICAgY29uc3QgdG9wVXJsID0gdG9wVVJJLnNwZWM7XG4gICAgICAgICAgICBjb25zdCBjaGFubmVsVVJJID0gZGV0YWlscy5VUkk7XG4gICAgICAgICAgICBjb25zdCBpc1RoaXJkUGFydHlUb1RvcFdpbmRvdyA9IFRoaXJkUGFydHlVdGlsLmlzVGhpcmRQYXJ0eVVSSShcbiAgICAgICAgICAgICAgY2hhbm5lbFVSSSxcbiAgICAgICAgICAgICAgdG9wVVJJLFxuICAgICAgICAgICAgKTtcbiAgICAgICAgICAgIHVwZGF0ZS5pc190aGlyZF9wYXJ0eV90b190b3Bfd2luZG93ID0gaXNUaGlyZFBhcnR5VG9Ub3BXaW5kb3c7XG4gICAgICAgICAgICB1cGRhdGUuaXNfdGhpcmRfcGFydHlfY2hhbm5lbCA9IGlzVGhpcmRQYXJ0eUNoYW5uZWw7XG4gICAgICAgICAgfVxuICAgICAgICB9IGNhdGNoIChhbkVycm9yKSB7XG4gICAgICAgICAgLy8gRXhjZXB0aW9ucyBleHBlY3RlZCBmb3IgY2hhbm5lbHMgdHJpZ2dlcmVkIG9yIGxvYWRpbmcgaW4gYVxuICAgICAgICAgIC8vIE51bGxQcmluY2lwYWwgb3IgU3lzdGVtUHJpbmNpcGFsLiBUaGV5IGFyZSBhbHNvIGV4cGVjdGVkIGZvciBmYXZpY29uXG4gICAgICAgICAgLy8gbG9hZHMsIHdoaWNoIHdlIGF0dGVtcHQgdG8gZmlsdGVyLiBEZXBlbmRpbmcgb24gdGhlIG5hbWluZywgc29tZSBmYXZpY29uc1xuICAgICAgICAgIC8vIG1heSBjb250aW51ZSB0byBsZWFkIHRvIGVycm9yIGxvZ3MuXG4gICAgICAgICAgaWYgKFxuICAgICAgICAgICAgdXBkYXRlLnRyaWdnZXJpbmdfb3JpZ2luICE9PSBcIltTeXN0ZW0gUHJpbmNpcGFsXVwiICYmXG4gICAgICAgICAgICB1cGRhdGUudHJpZ2dlcmluZ19vcmlnaW4gIT09IHVuZGVmaW5lZCAmJlxuICAgICAgICAgICAgdXBkYXRlLmxvYWRpbmdfb3JpZ2luICE9PSBcIltTeXN0ZW0gUHJpbmNpcGFsXVwiICYmXG4gICAgICAgICAgICB1cGRhdGUubG9hZGluZ19vcmlnaW4gIT09IHVuZGVmaW5lZCAmJlxuICAgICAgICAgICAgIXVwZGF0ZS51cmwuZW5kc1dpdGgoXCJpY29cIilcbiAgICAgICAgICApIHtcbiAgICAgICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLmxvZ0Vycm9yKFxuICAgICAgICAgICAgICBcIkVycm9yIHdoaWxlIHJldHJpZXZpbmcgYWRkaXRpb25hbCBjaGFubmVsIGluZm9ybWF0aW9uIGZvciBVUkw6IFwiICtcbiAgICAgICAgICAgICAgXCJcXG5cIiArXG4gICAgICAgICAgICAgIHVwZGF0ZS51cmwgK1xuICAgICAgICAgICAgICBcIlxcbiBFcnJvciB0ZXh0OlwiICtcbiAgICAgICAgICAgICAgSlNPTi5zdHJpbmdpZnkoYW5FcnJvciksXG4gICAgICAgICAgICApO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICAqL1xuICAgICAgICB1cGRhdGUudG9wX2xldmVsX3VybCA9IGVzY2FwZVVybCh0aGlzLmdldERvY3VtZW50VXJsRm9yUmVxdWVzdChkZXRhaWxzKSk7XG4gICAgICAgIHVwZGF0ZS5wYXJlbnRfZnJhbWVfaWQgPSBkZXRhaWxzLnBhcmVudEZyYW1lSWQ7XG4gICAgICAgIHVwZGF0ZS5mcmFtZV9hbmNlc3RvcnMgPSBlc2NhcGVTdHJpbmcoSlNPTi5zdHJpbmdpZnkoZGV0YWlscy5mcmFtZUFuY2VzdG9ycykpO1xuICAgICAgICB0aGlzLmRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwiaHR0cF9yZXF1ZXN0c1wiLCB1cGRhdGUpO1xuICAgIH1cbiAgICAvKipcbiAgICAgKiBDb2RlIHRha2VuIGFuZCBhZGFwdGVkIGZyb21cbiAgICAgKiBodHRwczovL2dpdGh1Yi5jb20vRUZGb3JnL3ByaXZhY3liYWRnZXIvcHVsbC8yMTk4L2ZpbGVzXG4gICAgICpcbiAgICAgKiBHZXRzIHRoZSBVUkwgZm9yIGEgZ2l2ZW4gcmVxdWVzdCdzIHRvcC1sZXZlbCBkb2N1bWVudC5cbiAgICAgKlxuICAgICAqIFRoZSByZXF1ZXN0J3MgZG9jdW1lbnQgbWF5IGJlIGRpZmZlcmVudCBmcm9tIHRoZSBjdXJyZW50IHRvcC1sZXZlbCBkb2N1bWVudFxuICAgICAqIGxvYWRlZCBpbiB0YWIgYXMgcmVxdWVzdHMgY2FuIGNvbWUgb3V0IG9mIG9yZGVyOlxuICAgICAqXG4gICAgICogQHBhcmFtIHtXZWJSZXF1ZXN0T25CZWZvcmVTZW5kSGVhZGVyc0V2ZW50RGV0YWlsc30gZGV0YWlsc1xuICAgICAqXG4gICAgICogQHJldHVybiB7P1N0cmluZ30gdGhlIFVSTCBmb3IgdGhlIHJlcXVlc3QncyB0b3AtbGV2ZWwgZG9jdW1lbnRcbiAgICAgKi9cbiAgICBnZXREb2N1bWVudFVybEZvclJlcXVlc3QoZGV0YWlscykge1xuICAgICAgICBsZXQgdXJsID0gXCJcIjtcbiAgICAgICAgaWYgKGRldGFpbHMudHlwZSA9PT0gXCJtYWluX2ZyYW1lXCIpIHtcbiAgICAgICAgICAgIC8vIFVybCBvZiB0aGUgdG9wLWxldmVsIGRvY3VtZW50IGl0c2VsZi5cbiAgICAgICAgICAgIHVybCA9IGRldGFpbHMudXJsO1xuICAgICAgICB9XG4gICAgICAgIGVsc2UgaWYgKGRldGFpbHMuaGFzT3duUHJvcGVydHkoXCJmcmFtZUFuY2VzdG9yc1wiKSkge1xuICAgICAgICAgICAgLy8gSW4gY2FzZSBvZiBuZXN0ZWQgZnJhbWVzLCByZXRyaWV2ZSB1cmwgZnJvbSB0b3AtbW9zdCBhbmNlc3Rvci5cbiAgICAgICAgICAgIC8vIElmIGZyYW1lQW5jZXN0b3JzID09IFtdLCByZXF1ZXN0IGNvbWVzIGZyb20gdGhlIHRvcC1sZXZlbC1kb2N1bWVudC5cbiAgICAgICAgICAgIHVybCA9IGRldGFpbHMuZnJhbWVBbmNlc3RvcnMubGVuZ3RoXG4gICAgICAgICAgICAgICAgPyBkZXRhaWxzLmZyYW1lQW5jZXN0b3JzW2RldGFpbHMuZnJhbWVBbmNlc3RvcnMubGVuZ3RoIC0gMV0udXJsXG4gICAgICAgICAgICAgICAgOiBkZXRhaWxzLmRvY3VtZW50VXJsO1xuICAgICAgICB9XG4gICAgICAgIGVsc2Uge1xuICAgICAgICAgICAgLy8gdHlwZSAhPSAnbWFpbl9mcmFtZScgYW5kIGZyYW1lQW5jZXN0b3JzID09IHVuZGVmaW5lZFxuICAgICAgICAgICAgLy8gRm9yIGV4YW1wbGUgc2VydmljZSB3b3JrZXJzOiBodHRwczovL2J1Z3ppbGxhLm1vemlsbGEub3JnL3Nob3dfYnVnLmNnaT9pZD0xNDcwNTM3I2MxM1xuICAgICAgICAgICAgdXJsID0gZGV0YWlscy5kb2N1bWVudFVybDtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gdXJsO1xuICAgIH1cbiAgICBhc3luYyBvbkJlZm9yZVJlZGlyZWN0SGFuZGxlcihkZXRhaWxzLCBjcmF3bElELCBldmVudE9yZGluYWwpIHtcbiAgICAgICAgLypcbiAgICAgICAgY29uc29sZS5sb2coXG4gICAgICAgICAgXCJvbkJlZm9yZVJlZGlyZWN0SGFuZGxlciAocHJldmlvdXNseSBodHRwUmVxdWVzdEhhbmRsZXIpXCIsXG4gICAgICAgICAgZGV0YWlscyxcbiAgICAgICAgICBjcmF3bElELFxuICAgICAgICApO1xuICAgICAgICAqL1xuICAgICAgICAvLyBTYXZlIEhUVFAgcmVkaXJlY3QgZXZlbnRzXG4gICAgICAgIC8vIEV2ZW50cyBhcmUgc2F2ZWQgdG8gdGhlIGBodHRwX3JlZGlyZWN0c2AgdGFibGVcbiAgICAgICAgLypcbiAgICAgICAgLy8gVE9ETzogUmVmYWN0b3IgdG8gY29ycmVzcG9uZGluZyB3ZWJleHQgbG9naWMgb3IgZGlzY2FyZFxuICAgICAgICAvLyBFdmVudHMgYXJlIHNhdmVkIHRvIHRoZSBgaHR0cF9yZWRpcmVjdHNgIHRhYmxlLCBhbmQgbWFwIHRoZSBvbGRcbiAgICAgICAgLy8gcmVxdWVzdC9yZXNwb25zZSBjaGFubmVsIGlkIHRvIHRoZSBuZXcgcmVxdWVzdC9yZXNwb25zZSBjaGFubmVsIGlkLlxuICAgICAgICAvLyBJbXBsZW1lbnRhdGlvbiBiYXNlZCBvbjogaHR0cHM6Ly9zdGFja292ZXJmbG93LmNvbS9hLzExMjQwNjI3XG4gICAgICAgIGNvbnN0IG9sZE5vdGlmaWNhdGlvbnMgPSBkZXRhaWxzLm5vdGlmaWNhdGlvbkNhbGxiYWNrcztcbiAgICAgICAgbGV0IG9sZEV2ZW50U2luayA9IG51bGw7XG4gICAgICAgIGRldGFpbHMubm90aWZpY2F0aW9uQ2FsbGJhY2tzID0ge1xuICAgICAgICAgIFF1ZXJ5SW50ZXJmYWNlOiBYUENPTVV0aWxzLmdlbmVyYXRlUUkoW1xuICAgICAgICAgICAgQ2kubnNJSW50ZXJmYWNlUmVxdWVzdG9yLFxuICAgICAgICAgICAgQ2kubnNJQ2hhbm5lbEV2ZW50U2luayxcbiAgICAgICAgICBdKSxcbiAgICBcbiAgICAgICAgICBnZXRJbnRlcmZhY2UoaWlkKSB7XG4gICAgICAgICAgICAvLyBXZSBhcmUgb25seSBpbnRlcmVzdGVkIGluIG5zSUNoYW5uZWxFdmVudFNpbmssXG4gICAgICAgICAgICAvLyByZXR1cm4gdGhlIG9sZCBjYWxsYmFja3MgZm9yIGFueSBvdGhlciBpbnRlcmZhY2UgcmVxdWVzdHMuXG4gICAgICAgICAgICBpZiAoaWlkLmVxdWFscyhDaS5uc0lDaGFubmVsRXZlbnRTaW5rKSkge1xuICAgICAgICAgICAgICB0cnkge1xuICAgICAgICAgICAgICAgIG9sZEV2ZW50U2luayA9IG9sZE5vdGlmaWNhdGlvbnMuUXVlcnlJbnRlcmZhY2UoaWlkKTtcbiAgICAgICAgICAgICAgfSBjYXRjaCAoYW5FcnJvcikge1xuICAgICAgICAgICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLmxvZ0Vycm9yKFxuICAgICAgICAgICAgICAgICAgXCJFcnJvciBkdXJpbmcgY2FsbCB0byBjdXN0b20gbm90aWZpY2F0aW9uQ2FsbGJhY2tzOjpnZXRJbnRlcmZhY2UuXCIgK1xuICAgICAgICAgICAgICAgICAgICBKU09OLnN0cmluZ2lmeShhbkVycm9yKSxcbiAgICAgICAgICAgICAgICApO1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIHJldHVybiB0aGlzO1xuICAgICAgICAgICAgfVxuICAgIFxuICAgICAgICAgICAgaWYgKG9sZE5vdGlmaWNhdGlvbnMpIHtcbiAgICAgICAgICAgICAgcmV0dXJuIG9sZE5vdGlmaWNhdGlvbnMuZ2V0SW50ZXJmYWNlKGlpZCk7XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICB0aHJvdyBDci5OU19FUlJPUl9OT19JTlRFUkZBQ0U7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfSxcbiAgICBcbiAgICAgICAgICBhc3luY09uQ2hhbm5lbFJlZGlyZWN0KG9sZENoYW5uZWwsIG5ld0NoYW5uZWwsIGZsYWdzLCBjYWxsYmFjaykge1xuICAgIFxuICAgICAgICAgICAgbmV3Q2hhbm5lbC5RdWVyeUludGVyZmFjZShDaS5uc0lIdHRwQ2hhbm5lbCk7XG4gICAgXG4gICAgICAgICAgICBjb25zdCBodHRwUmVkaXJlY3Q6IEh0dHBSZWRpcmVjdCA9IHtcbiAgICAgICAgICAgICAgYnJvd3Nlcl9pZDogY3Jhd2xJRCxcbiAgICAgICAgICAgICAgb2xkX3JlcXVlc3RfaWQ6IG9sZENoYW5uZWwuY2hhbm5lbElkLFxuICAgICAgICAgICAgICBuZXdfcmVxdWVzdF9pZDogbmV3Q2hhbm5lbC5jaGFubmVsSWQsXG4gICAgICAgICAgICAgIHRpbWVfc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgICAgIH07XG4gICAgICAgICAgICB0aGlzLmRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwiaHR0cF9yZWRpcmVjdHNcIiwgaHR0cFJlZGlyZWN0KTtcbiAgICBcbiAgICAgICAgICAgIGlmIChvbGRFdmVudFNpbmspIHtcbiAgICAgICAgICAgICAgb2xkRXZlbnRTaW5rLmFzeW5jT25DaGFubmVsUmVkaXJlY3QoXG4gICAgICAgICAgICAgICAgb2xkQ2hhbm5lbCxcbiAgICAgICAgICAgICAgICBuZXdDaGFubmVsLFxuICAgICAgICAgICAgICAgIGZsYWdzLFxuICAgICAgICAgICAgICAgIGNhbGxiYWNrLFxuICAgICAgICAgICAgICApO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgY2FsbGJhY2sub25SZWRpcmVjdFZlcmlmeUNhbGxiYWNrKENyLk5TX09LKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9LFxuICAgICAgICB9O1xuICAgICAgICAqL1xuICAgICAgICBjb25zdCByZXNwb25zZVN0YXR1cyA9IGRldGFpbHMuc3RhdHVzQ29kZTtcbiAgICAgICAgY29uc3QgcmVzcG9uc2VTdGF0dXNUZXh0ID0gZGV0YWlscy5zdGF0dXNMaW5lO1xuICAgICAgICBjb25zdCB0YWIgPSBkZXRhaWxzLnRhYklkID4gLTFcbiAgICAgICAgICAgID8gYXdhaXQgYnJvd3Nlci50YWJzLmdldChkZXRhaWxzLnRhYklkKVxuICAgICAgICAgICAgOiB7IHdpbmRvd0lkOiB1bmRlZmluZWQsIGluY29nbml0bzogdW5kZWZpbmVkIH07XG4gICAgICAgIGNvbnN0IGh0dHBSZWRpcmVjdCA9IHtcbiAgICAgICAgICAgIGluY29nbml0bzogYm9vbFRvSW50KHRhYi5pbmNvZ25pdG8pLFxuICAgICAgICAgICAgYnJvd3Nlcl9pZDogY3Jhd2xJRCxcbiAgICAgICAgICAgIG9sZF9yZXF1ZXN0X3VybDogZXNjYXBlVXJsKGRldGFpbHMudXJsKSxcbiAgICAgICAgICAgIG9sZF9yZXF1ZXN0X2lkOiBkZXRhaWxzLnJlcXVlc3RJZCxcbiAgICAgICAgICAgIG5ld19yZXF1ZXN0X3VybDogZXNjYXBlVXJsKGRldGFpbHMucmVkaXJlY3RVcmwpLFxuICAgICAgICAgICAgbmV3X3JlcXVlc3RfaWQ6IG51bGwsXG4gICAgICAgICAgICBleHRlbnNpb25fc2Vzc2lvbl91dWlkOiBleHRlbnNpb25TZXNzaW9uVXVpZCxcbiAgICAgICAgICAgIGV2ZW50X29yZGluYWw6IGV2ZW50T3JkaW5hbCxcbiAgICAgICAgICAgIHdpbmRvd19pZDogdGFiLndpbmRvd0lkLFxuICAgICAgICAgICAgdGFiX2lkOiBkZXRhaWxzLnRhYklkLFxuICAgICAgICAgICAgZnJhbWVfaWQ6IGRldGFpbHMuZnJhbWVJZCxcbiAgICAgICAgICAgIHJlc3BvbnNlX3N0YXR1czogcmVzcG9uc2VTdGF0dXMsXG4gICAgICAgICAgICByZXNwb25zZV9zdGF0dXNfdGV4dDogZXNjYXBlU3RyaW5nKHJlc3BvbnNlU3RhdHVzVGV4dCksXG4gICAgICAgICAgICBoZWFkZXJzOiB0aGlzLmpzb25pZnlIZWFkZXJzKGRldGFpbHMucmVzcG9uc2VIZWFkZXJzKS5oZWFkZXJzLFxuICAgICAgICAgICAgdGltZV9zdGFtcDogbmV3IERhdGUoZGV0YWlscy50aW1lU3RhbXApLnRvSVNPU3RyaW5nKCksXG4gICAgICAgIH07XG4gICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLnNhdmVSZWNvcmQoXCJodHRwX3JlZGlyZWN0c1wiLCBodHRwUmVkaXJlY3QpO1xuICAgIH1cbiAgICAvKlxuICAgICAqIEhUVFAgUmVzcG9uc2UgSGFuZGxlcnMgYW5kIEhlbHBlciBGdW5jdGlvbnNcbiAgICAgKi9cbiAgICBhc3luYyBsb2dXaXRoUmVzcG9uc2VCb2R5KGRldGFpbHMsIHVwZGF0ZSkge1xuICAgICAgICBjb25zdCBwZW5kaW5nUmVzcG9uc2UgPSB0aGlzLmdldFBlbmRpbmdSZXNwb25zZShkZXRhaWxzLnJlcXVlc3RJZCk7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgICBjb25zdCByZXNwb25zZUJvZHlMaXN0ZW5lciA9IHBlbmRpbmdSZXNwb25zZS5yZXNwb25zZUJvZHlMaXN0ZW5lcjtcbiAgICAgICAgICAgIGNvbnN0IHJlc3BCb2R5ID0gYXdhaXQgcmVzcG9uc2VCb2R5TGlzdGVuZXIuZ2V0UmVzcG9uc2VCb2R5KCk7XG4gICAgICAgICAgICBjb25zdCBjb250ZW50SGFzaCA9IGF3YWl0IHJlc3BvbnNlQm9keUxpc3RlbmVyLmdldENvbnRlbnRIYXNoKCk7XG4gICAgICAgICAgICB0aGlzLmRhdGFSZWNlaXZlci5zYXZlQ29udGVudChyZXNwQm9keSwgZXNjYXBlU3RyaW5nKGNvbnRlbnRIYXNoKSk7XG4gICAgICAgICAgICB1cGRhdGUuY29udGVudF9oYXNoID0gY29udGVudEhhc2g7XG4gICAgICAgICAgICB0aGlzLmRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwiaHR0cF9yZXNwb25zZXNcIiwgdXBkYXRlKTtcbiAgICAgICAgfVxuICAgICAgICBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgICAvKlxuICAgICAgICAgICAgLy8gVE9ETzogUmVmYWN0b3IgdG8gY29ycmVzcG9uZGluZyB3ZWJleHQgbG9naWMgb3IgZGlzY2FyZFxuICAgICAgICAgICAgZGF0YVJlY2VpdmVyLmxvZ0Vycm9yKFxuICAgICAgICAgICAgICBcIlVuYWJsZSB0byByZXRyaWV2ZSByZXNwb25zZSBib2R5LlwiICsgSlNPTi5zdHJpbmdpZnkoYVJlYXNvbiksXG4gICAgICAgICAgICApO1xuICAgICAgICAgICAgdXBkYXRlLmNvbnRlbnRfaGFzaCA9IFwiPGVycm9yPlwiO1xuICAgICAgICAgICAgZGF0YVJlY2VpdmVyLnNhdmVSZWNvcmQoXCJodHRwX3Jlc3BvbnNlc1wiLCB1cGRhdGUpO1xuICAgICAgICAgICAgKi9cbiAgICAgICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLmxvZ0Vycm9yKFwiVW5hYmxlIHRvIHJldHJpZXZlIHJlc3BvbnNlIGJvZHkuXCIgK1xuICAgICAgICAgICAgICAgIFwiTGlrZWx5IGNhdXNlZCBieSBhIHByb2dyYW1taW5nIGVycm9yLiBFcnJvciBNZXNzYWdlOlwiICtcbiAgICAgICAgICAgICAgICBlcnIubmFtZSArXG4gICAgICAgICAgICAgICAgZXJyLm1lc3NhZ2UgK1xuICAgICAgICAgICAgICAgIFwiXFxuXCIgK1xuICAgICAgICAgICAgICAgIGVyci5zdGFjayk7XG4gICAgICAgICAgICB1cGRhdGUuY29udGVudF9oYXNoID0gXCI8ZXJyb3I+XCI7XG4gICAgICAgICAgICB0aGlzLmRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwiaHR0cF9yZXNwb25zZXNcIiwgdXBkYXRlKTtcbiAgICAgICAgfVxuICAgIH1cbiAgICAvLyBJbnN0cnVtZW50IEhUVFAgcmVzcG9uc2VzXG4gICAgYXN5bmMgb25Db21wbGV0ZWRIYW5kbGVyKGRldGFpbHMsIGNyYXdsSUQsIGV2ZW50T3JkaW5hbCwgc2F2ZUNvbnRlbnQpIHtcbiAgICAgICAgLypcbiAgICAgICAgY29uc29sZS5sb2coXG4gICAgICAgICAgXCJvbkNvbXBsZXRlZEhhbmRsZXIgKHByZXZpb3VzbHkgaHR0cFJlcXVlc3RIYW5kbGVyKVwiLFxuICAgICAgICAgIGRldGFpbHMsXG4gICAgICAgICAgY3Jhd2xJRCxcbiAgICAgICAgICBzYXZlQ29udGVudCxcbiAgICAgICAgKTtcbiAgICAgICAgKi9cbiAgICAgICAgY29uc3QgdGFiID0gZGV0YWlscy50YWJJZCA+IC0xXG4gICAgICAgICAgICA/IGF3YWl0IGJyb3dzZXIudGFicy5nZXQoZGV0YWlscy50YWJJZClcbiAgICAgICAgICAgIDogeyB3aW5kb3dJZDogdW5kZWZpbmVkLCBpbmNvZ25pdG86IHVuZGVmaW5lZCB9O1xuICAgICAgICBjb25zdCB1cGRhdGUgPSB7fTtcbiAgICAgICAgdXBkYXRlLmluY29nbml0byA9IGJvb2xUb0ludCh0YWIuaW5jb2duaXRvKTtcbiAgICAgICAgdXBkYXRlLmJyb3dzZXJfaWQgPSBjcmF3bElEO1xuICAgICAgICB1cGRhdGUuZXh0ZW5zaW9uX3Nlc3Npb25fdXVpZCA9IGV4dGVuc2lvblNlc3Npb25VdWlkO1xuICAgICAgICB1cGRhdGUuZXZlbnRfb3JkaW5hbCA9IGV2ZW50T3JkaW5hbDtcbiAgICAgICAgdXBkYXRlLndpbmRvd19pZCA9IHRhYi53aW5kb3dJZDtcbiAgICAgICAgdXBkYXRlLnRhYl9pZCA9IGRldGFpbHMudGFiSWQ7XG4gICAgICAgIHVwZGF0ZS5mcmFtZV9pZCA9IGRldGFpbHMuZnJhbWVJZDtcbiAgICAgICAgLy8gcmVxdWVzdElkIGlzIGEgdW5pcXVlIGlkZW50aWZpZXIgdGhhdCBjYW4gYmUgdXNlZCB0byBsaW5rIHJlcXVlc3RzIGFuZCByZXNwb25zZXNcbiAgICAgICAgdXBkYXRlLnJlcXVlc3RfaWQgPSBOdW1iZXIoZGV0YWlscy5yZXF1ZXN0SWQpO1xuICAgICAgICBjb25zdCBpc0NhY2hlZCA9IGRldGFpbHMuZnJvbUNhY2hlO1xuICAgICAgICB1cGRhdGUuaXNfY2FjaGVkID0gYm9vbFRvSW50KGlzQ2FjaGVkKTtcbiAgICAgICAgY29uc3QgdXJsID0gZGV0YWlscy51cmw7XG4gICAgICAgIHVwZGF0ZS51cmwgPSBlc2NhcGVVcmwodXJsKTtcbiAgICAgICAgY29uc3QgcmVxdWVzdE1ldGhvZCA9IGRldGFpbHMubWV0aG9kO1xuICAgICAgICB1cGRhdGUubWV0aG9kID0gZXNjYXBlU3RyaW5nKHJlcXVlc3RNZXRob2QpO1xuICAgICAgICAvLyBUT0RPOiBSZWZhY3RvciB0byBjb3JyZXNwb25kaW5nIHdlYmV4dCBsb2dpYyBvciBkaXNjYXJkXG4gICAgICAgIC8vIChyZXF1ZXN0IGhlYWRlcnMgYXJlIG5vdCBhdmFpbGFibGUgaW4gaHR0cCByZXNwb25zZSBldmVudCBsaXN0ZW5lciBvYmplY3QsXG4gICAgICAgIC8vIGJ1dCB0aGUgcmVmZXJyZXIgcHJvcGVydHkgb2YgdGhlIGNvcnJlc3BvbmRpbmcgcmVxdWVzdCBjb3VsZCBiZSBxdWVyaWVkKVxuICAgICAgICAvL1xuICAgICAgICAvLyBsZXQgcmVmZXJyZXIgPSBcIlwiO1xuICAgICAgICAvLyBpZiAoZGV0YWlscy5yZWZlcnJlcikge1xuICAgICAgICAvLyAgIHJlZmVycmVyID0gZGV0YWlscy5yZWZlcnJlci5zcGVjO1xuICAgICAgICAvLyB9XG4gICAgICAgIC8vIHVwZGF0ZS5yZWZlcnJlciA9IGVzY2FwZVN0cmluZyhyZWZlcnJlcik7XG4gICAgICAgIGNvbnN0IHJlc3BvbnNlU3RhdHVzID0gZGV0YWlscy5zdGF0dXNDb2RlO1xuICAgICAgICB1cGRhdGUucmVzcG9uc2Vfc3RhdHVzID0gcmVzcG9uc2VTdGF0dXM7XG4gICAgICAgIGNvbnN0IHJlc3BvbnNlU3RhdHVzVGV4dCA9IGRldGFpbHMuc3RhdHVzTGluZTtcbiAgICAgICAgdXBkYXRlLnJlc3BvbnNlX3N0YXR1c190ZXh0ID0gZXNjYXBlU3RyaW5nKHJlc3BvbnNlU3RhdHVzVGV4dCk7XG4gICAgICAgIGNvbnN0IGN1cnJlbnRfdGltZSA9IG5ldyBEYXRlKGRldGFpbHMudGltZVN0YW1wKTtcbiAgICAgICAgdXBkYXRlLnRpbWVfc3RhbXAgPSBjdXJyZW50X3RpbWUudG9JU09TdHJpbmcoKTtcbiAgICAgICAgY29uc3QgcGFyc2VkSGVhZGVycyA9IHRoaXMuanNvbmlmeUhlYWRlcnMoZGV0YWlscy5yZXNwb25zZUhlYWRlcnMpO1xuICAgICAgICB1cGRhdGUuaGVhZGVycyA9IHBhcnNlZEhlYWRlcnMuaGVhZGVycztcbiAgICAgICAgdXBkYXRlLmxvY2F0aW9uID0gcGFyc2VkSGVhZGVycy5sb2NhdGlvbjtcbiAgICAgICAgaWYgKHRoaXMuc2hvdWxkU2F2ZUNvbnRlbnQoc2F2ZUNvbnRlbnQsIGRldGFpbHMudHlwZSkpIHtcbiAgICAgICAgICAgIHRoaXMubG9nV2l0aFJlc3BvbnNlQm9keShkZXRhaWxzLCB1cGRhdGUpO1xuICAgICAgICB9XG4gICAgICAgIGVsc2Uge1xuICAgICAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIuc2F2ZVJlY29yZChcImh0dHBfcmVzcG9uc2VzXCIsIHVwZGF0ZSk7XG4gICAgICAgIH1cbiAgICB9XG4gICAganNvbmlmeUhlYWRlcnMoaGVhZGVycykge1xuICAgICAgICBjb25zdCByZXN1bHRIZWFkZXJzID0gW107XG4gICAgICAgIGxldCBsb2NhdGlvbiA9IFwiXCI7XG4gICAgICAgIGlmIChoZWFkZXJzKSB7XG4gICAgICAgICAgICBoZWFkZXJzLm1hcCgocmVzcG9uc2VIZWFkZXIpID0+IHtcbiAgICAgICAgICAgICAgICBjb25zdCB7IG5hbWUsIHZhbHVlIH0gPSByZXNwb25zZUhlYWRlcjtcbiAgICAgICAgICAgICAgICBjb25zdCBoZWFkZXJfcGFpciA9IFtdO1xuICAgICAgICAgICAgICAgIGhlYWRlcl9wYWlyLnB1c2goZXNjYXBlU3RyaW5nKG5hbWUpKTtcbiAgICAgICAgICAgICAgICBoZWFkZXJfcGFpci5wdXNoKGVzY2FwZVN0cmluZyh2YWx1ZSkpO1xuICAgICAgICAgICAgICAgIHJlc3VsdEhlYWRlcnMucHVzaChoZWFkZXJfcGFpcik7XG4gICAgICAgICAgICAgICAgaWYgKG5hbWUudG9Mb3dlckNhc2UoKSA9PT0gXCJsb2NhdGlvblwiKSB7XG4gICAgICAgICAgICAgICAgICAgIGxvY2F0aW9uID0gdmFsdWU7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICAgIGhlYWRlcnM6IEpTT04uc3RyaW5naWZ5KHJlc3VsdEhlYWRlcnMpLFxuICAgICAgICAgICAgbG9jYXRpb246IGVzY2FwZVN0cmluZyhsb2NhdGlvbiksXG4gICAgICAgIH07XG4gICAgfVxufVxuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pYUhSMGNDMXBibk4wY25WdFpXNTBMbXB6SWl3aWMyOTFjbU5sVW05dmRDSTZJaUlzSW5OdmRYSmpaWE1pT2xzaUxpNHZMaTR2TGk0dmMzSmpMMkpoWTJ0bmNtOTFibVF2YUhSMGNDMXBibk4wY25WdFpXNTBMblJ6SWwwc0ltNWhiV1Z6SWpwYlhTd2liV0Z3Y0dsdVozTWlPaUpCUVVGQkxFOUJRVThzUlVGQlJTeDFRa0ZCZFVJc1JVRkJSU3hOUVVGTkxIZERRVUYzUXl4RFFVRkRPMEZCUTJwR0xFOUJRVThzUlVGQlJTeHZRa0ZCYjBJc1JVRkJSU3hOUVVGTkxDdENRVUVyUWl4RFFVRkRPMEZCUTNKRkxFOUJRVThzUlVGQlJTeGpRVUZqTEVWQlFYRkNMRTFCUVUwc2VVSkJRWGxDTEVOQlFVTTdRVUZETlVVc1QwRkJUeXhGUVVGRkxHTkJRV01zUlVGQlJTeE5RVUZOTEhkQ1FVRjNRaXhEUVVGRE8wRkJRM2hFTEU5QlFVOHNSVUZCUlN4bFFVRmxMRVZCUVVVc1RVRkJUU3g1UWtGQmVVSXNRMEZCUXp0QlFVTXhSQ3hQUVVGUExFVkJRVVVzVTBGQlV5eEZRVUZGTEZsQlFWa3NSVUZCUlN4VFFVRlRMRVZCUVVVc1RVRkJUU3h4UWtGQmNVSXNRMEZCUXp0QlFXVjZSVHM3T3pzN08wZEJUVWM3UVVGRlNDeE5RVUZOTEZGQlFWRXNSMEZCYlVJN1NVRkRMMElzVVVGQlVUdEpRVU5TTEZsQlFWazdTVUZEV2l4TlFVRk5PMGxCUTA0c1QwRkJUenRKUVVOUUxGVkJRVlU3U1VGRFZpeFpRVUZaTzBsQlExb3NUMEZCVHp0SlFVTlFMRkZCUVZFN1NVRkRVaXh0UWtGQmJVSTdTVUZEYmtJc1RVRkJUVHRKUVVOT0xGRkJRVkU3U1VGRFVpeGhRVUZoTzBsQlEySXNXVUZCV1R0SlFVTmFMRmRCUVZjN1NVRkRXQ3hqUVVGak8wbEJRMlFzVjBGQlZ6dEpRVU5ZTEZOQlFWTTdTVUZEVkN4blFrRkJaMEk3U1VGRGFFSXNUVUZCVFR0SlFVTk9MRTlCUVU4N1EwRkRVaXhEUVVGRE8wRkJSVVlzVDBGQlR5eEZRVUZGTEZGQlFWRXNSVUZCUlN4RFFVRkRPMEZCUlhCQ0xFMUJRVTBzVDBGQlR5eGpRVUZqTzBsQlExSXNXVUZCV1N4RFFVRkRPMGxCUTNSQ0xHVkJRV1VzUjBGRmJrSXNSVUZCUlN4RFFVRkRPMGxCUTBNc1owSkJRV2RDTEVkQlJYQkNMRVZCUVVVc1EwRkJRenRKUVVORExIVkNRVUYxUWl4RFFVRkRPMGxCUTNoQ0xESkNRVUV5UWl4RFFVRkRPMGxCUXpWQ0xIZENRVUYzUWl4RFFVRkRPMGxCUTNwQ0xHMUNRVUZ0UWl4RFFVRkRPMGxCUlRWQ0xGbEJRVmtzV1VGQldUdFJRVU4wUWl4SlFVRkpMRU5CUVVNc1dVRkJXU3hIUVVGSExGbEJRVmtzUTBGQlF6dEpRVU51UXl4RFFVRkRPMGxCUlUwc1IwRkJSeXhEUVVGRExFOUJRVThzUlVGQlJTeHBRa0ZCYjBNN1VVRkRkRVFzVFVGQlRTeE5RVUZOTEVkQlFXdENMRVZCUVVVc1NVRkJTU3hGUVVGRkxFTkJRVU1zV1VGQldTeERRVUZETEVWQlFVVXNTMEZCU3l4RlFVRkZMRkZCUVZFc1JVRkJSU3hEUVVGRE8xRkJSWGhGTEUxQlFVMHNlVUpCUVhsQ0xFZEJRVWNzUTBGQlF5eFBRVUZQTEVWQlFVVXNSVUZCUlR0WlFVTTFReXhQUVVGUExFTkJRMHdzVDBGQlR5eERRVUZETEZOQlFWTXNTVUZCU1N4UFFVRlBMRU5CUVVNc1UwRkJVeXhEUVVGRExFOUJRVThzUTBGQlF5eHJRa0ZCYTBJc1EwRkJReXhIUVVGSExFTkJRVU1zUTBGQlF5eERRVU40UlN4RFFVRkRPMUZCUTBvc1EwRkJReXhEUVVGRE8xRkJSVVk3TzFkQlJVYzdVVUZGU0N4SlFVRkpMRU5CUVVNc2RVSkJRWFZDTEVkQlFVY3NRMEZETjBJc1QwRkJPRU1zUlVGRE9VTXNSVUZCUlR0WlFVTkdMRTFCUVUwc0swSkJRU3RDTEVkQlFYRkNMRVZCUVVVc1EwRkJRenRaUVVNM1JDeHhRMEZCY1VNN1dVRkRja01zU1VGQlNTeDVRa0ZCZVVJc1EwRkJReXhQUVVGUExFTkJRVU1zUlVGQlJUdG5Ra0ZEZEVNc1QwRkJUeXdyUWtGQkswSXNRMEZCUXp0aFFVTjRRenRaUVVORUxFMUJRVTBzWTBGQll5eEhRVUZITEVsQlFVa3NRMEZCUXl4cFFrRkJhVUlzUTBGQlF5eFBRVUZQTEVOQlFVTXNVMEZCVXl4RFFVRkRMRU5CUVVNN1dVRkRha1VzWTBGQll5eERRVUZETEd0RFFVRnJReXhEUVVGRExFOUJRVThzUTBGQlF5eERRVUZETzFsQlF6TkVMRTFCUVUwc1pVRkJaU3hIUVVGSExFbEJRVWtzUTBGQlF5eHJRa0ZCYTBJc1EwRkJReXhQUVVGUExFTkJRVU1zVTBGQlV5eERRVUZETEVOQlFVTTdXVUZEYmtVc1pVRkJaU3hEUVVGRExHdERRVUZyUXl4RFFVRkRMRTlCUVU4c1EwRkJReXhEUVVGRE8xbEJRelZFTEVsQlFVa3NTVUZCU1N4RFFVRkRMR2xDUVVGcFFpeERRVUZETEdsQ1FVRnBRaXhGUVVGRkxFOUJRVThzUTBGQlF5eEpRVUZKTEVOQlFVTXNSVUZCUlR0blFrRkRNMFFzWlVGQlpTeERRVUZETEN0Q1FVRXJRaXhEUVVGRExFOUJRVThzUTBGQlF5eERRVUZETzJGQlF6RkVPMWxCUTBRc1QwRkJUeXdyUWtGQkswSXNRMEZCUXp0UlFVTjZReXhEUVVGRExFTkJRVU03VVVGRFJpeFBRVUZQTEVOQlFVTXNWVUZCVlN4RFFVRkRMR1ZCUVdVc1EwRkJReXhYUVVGWExFTkJRelZETEVsQlFVa3NRMEZCUXl4MVFrRkJkVUlzUlVGRE5VSXNUVUZCVFN4RlFVTk9MRWxCUVVrc1EwRkJReXh6UWtGQmMwSXNRMEZCUXl4cFFrRkJhVUlzUTBGQlF6dFpRVU0xUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhoUVVGaExFVkJRVVVzVlVGQlZTeERRVUZETzFsQlF6ZENMRU5CUVVNc1EwRkJReXhEUVVGRExHRkJRV0VzUTBGQlF5eERRVU53UWl4RFFVRkRPMUZCUlVZc1NVRkJTU3hEUVVGRExESkNRVUV5UWl4SFFVRkhMRU5CUVVNc1QwRkJUeXhGUVVGRkxFVkJRVVU3V1VGRE4wTXNjVU5CUVhGRE8xbEJRM0pETEVsQlFVa3NlVUpCUVhsQ0xFTkJRVU1zVDBGQlR5eERRVUZETEVWQlFVVTdaMEpCUTNSRExFOUJRVTg3WVVGRFVqdFpRVU5FTEUxQlFVMHNZMEZCWXl4SFFVRkhMRWxCUVVrc1EwRkJReXhwUWtGQmFVSXNRMEZCUXl4UFFVRlBMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU03V1VGRGFrVXNZMEZCWXl4RFFVRkRMSE5EUVVGelF5eERRVUZETEU5QlFVOHNRMEZCUXl4RFFVRkRPMWxCUXk5RUxFbEJRVWtzUTBGQlF5d3dRa0ZCTUVJc1EwRkROMElzVDBGQlR5eEZRVU5RTEU5QlFVOHNSVUZEVUN4MVFrRkJkVUlzUlVGQlJTeERRVU14UWl4RFFVRkRPMUZCUTBvc1EwRkJReXhEUVVGRE8xRkJRMFlzVDBGQlR5eERRVUZETEZWQlFWVXNRMEZCUXl4dFFrRkJiVUlzUTBGQlF5eFhRVUZYTEVOQlEyaEVMRWxCUVVrc1EwRkJReXd5UWtGQk1rSXNSVUZEYUVNc1RVRkJUU3hGUVVOT0xFTkJRVU1zWjBKQlFXZENMRU5CUVVNc1EwRkRia0lzUTBGQlF6dFJRVVZHTEVsQlFVa3NRMEZCUXl4M1FrRkJkMElzUjBGQlJ5eERRVUZETEU5QlFVOHNSVUZCUlN4RlFVRkZPMWxCUXpGRExIRkRRVUZ4UXp0WlFVTnlReXhKUVVGSkxIbENRVUY1UWl4RFFVRkRMRTlCUVU4c1EwRkJReXhGUVVGRk8yZENRVU4wUXl4UFFVRlBPMkZCUTFJN1dVRkRSQ3hKUVVGSkxFTkJRVU1zZFVKQlFYVkNMRU5CUVVNc1QwRkJUeXhGUVVGRkxFOUJRVThzUlVGQlJTeDFRa0ZCZFVJc1JVRkJSU3hEUVVGRExFTkJRVU03VVVGRE5VVXNRMEZCUXl4RFFVRkRPMUZCUTBZc1QwRkJUeXhEUVVGRExGVkJRVlVzUTBGQlF5eG5Ra0ZCWjBJc1EwRkJReXhYUVVGWExFTkJRemRETEVsQlFVa3NRMEZCUXl4M1FrRkJkMElzUlVGRE4wSXNUVUZCVFN4RlFVTk9MRU5CUVVNc2FVSkJRV2xDTEVOQlFVTXNRMEZEY0VJc1EwRkJRenRSUVVWR0xFbEJRVWtzUTBGQlF5eHRRa0ZCYlVJc1IwRkJSeXhEUVVGRExFOUJRVThzUlVGQlJTeEZRVUZGTzFsQlEzSkRMSEZEUVVGeFF6dFpRVU55UXl4SlFVRkpMSGxDUVVGNVFpeERRVUZETEU5QlFVOHNRMEZCUXl4RlFVRkZPMmRDUVVOMFF5eFBRVUZQTzJGQlExSTdXVUZEUkN4TlFVRk5MR1ZCUVdVc1IwRkJSeXhKUVVGSkxFTkJRVU1zYTBKQlFXdENMRU5CUVVNc1QwRkJUeXhEUVVGRExGTkJRVk1zUTBGQlF5eERRVUZETzFsQlEyNUZMR1ZCUVdVc1EwRkJReXc0UWtGQk9FSXNRMEZCUXl4UFFVRlBMRU5CUVVNc1EwRkJRenRaUVVONFJDeEpRVUZKTEVOQlFVTXNhMEpCUVd0Q0xFTkJRM0pDTEU5QlFVOHNSVUZEVUN4UFFVRlBMRVZCUTFBc2RVSkJRWFZDTEVWQlFVVXNSVUZEZWtJc2FVSkJRV2xDTEVOQlEyeENMRU5CUVVNN1VVRkRTaXhEUVVGRExFTkJRVU03VVVGRFJpeFBRVUZQTEVOQlFVTXNWVUZCVlN4RFFVRkRMRmRCUVZjc1EwRkJReXhYUVVGWExFTkJRM2hETEVsQlFVa3NRMEZCUXl4dFFrRkJiVUlzUlVGRGVFSXNUVUZCVFN4RlFVTk9MRU5CUVVNc2FVSkJRV2xDTEVOQlFVTXNRMEZEY0VJc1EwRkJRenRKUVVOS0xFTkJRVU03U1VGRlRTeFBRVUZQTzFGQlExb3NTVUZCU1N4SlFVRkpMRU5CUVVNc2RVSkJRWFZDTEVWQlFVVTdXVUZEYUVNc1QwRkJUeXhEUVVGRExGVkJRVlVzUTBGQlF5eGxRVUZsTEVOQlFVTXNZMEZCWXl4RFFVTXZReXhKUVVGSkxFTkJRVU1zZFVKQlFYVkNMRU5CUXpkQ0xFTkJRVU03VTBGRFNEdFJRVU5FTEVsQlFVa3NTVUZCU1N4RFFVRkRMREpDUVVFeVFpeEZRVUZGTzFsQlEzQkRMRTlCUVU4c1EwRkJReXhWUVVGVkxFTkJRVU1zYlVKQlFXMUNMRU5CUVVNc1kwRkJZeXhEUVVOdVJDeEpRVUZKTEVOQlFVTXNNa0pCUVRKQ0xFTkJRMnBETEVOQlFVTTdVMEZEU0R0UlFVTkVMRWxCUVVrc1NVRkJTU3hEUVVGRExIZENRVUYzUWl4RlFVRkZPMWxCUTJwRExFOUJRVThzUTBGQlF5eFZRVUZWTEVOQlFVTXNaMEpCUVdkQ0xFTkJRVU1zWTBGQll5eERRVU5vUkN4SlFVRkpMRU5CUVVNc2QwSkJRWGRDTEVOQlF6bENMRU5CUVVNN1UwRkRTRHRSUVVORUxFbEJRVWtzU1VGQlNTeERRVUZETEcxQ1FVRnRRaXhGUVVGRk8xbEJRelZDTEU5QlFVOHNRMEZCUXl4VlFVRlZMRU5CUVVNc1YwRkJWeXhEUVVGRExHTkJRV01zUTBGQlF5eEpRVUZKTEVOQlFVTXNiVUpCUVcxQ0xFTkJRVU1zUTBGQlF6dFRRVU42UlR0SlFVTklMRU5CUVVNN1NVRkZUeXh6UWtGQmMwSXNRMEZCUXl4cFFrRkJiME03VVVGRGFrVXNTVUZCU1N4cFFrRkJhVUlzUzBGQlN5eEpRVUZKTEVWQlFVVTdXVUZET1VJc1QwRkJUeXhKUVVGSkxFTkJRVU03VTBGRFlqdFJRVU5FTEVsQlFVa3NhVUpCUVdsQ0xFdEJRVXNzUzBGQlN5eEZRVUZGTzFsQlF5OUNMRTlCUVU4c1MwRkJTeXhEUVVGRE8xTkJRMlE3VVVGRFJDeFBRVUZQTEVsQlFVa3NRMEZCUXl4M1FrRkJkMElzUTBGQlF5eHBRa0ZCYVVJc1EwRkJReXhEUVVGRExFMUJRVTBzUjBGQlJ5eERRVUZETEVOQlFVTTdTVUZEY2tVc1EwRkJRenRKUVVWUExIZENRVUYzUWl4RFFVRkRMR2xDUVVGNVFqdFJRVU40UkN4UFFVRlBMR2xDUVVGcFFpeERRVUZETEV0QlFVc3NRMEZCUXl4SFFVRkhMRU5CUVcxQ0xFTkJRVU03U1VGRGVFUXNRMEZCUXp0SlFVVkVPenM3T3pzN1QwRk5SenRKUVVOTExHbENRVUZwUWl4RFFVTjJRaXhwUWtGQmIwTXNSVUZEY0VNc1dVRkJNRUk3VVVGRk1VSXNTVUZCU1N4cFFrRkJhVUlzUzBGQlN5eEpRVUZKTEVWQlFVVTdXVUZET1VJc1QwRkJUeXhKUVVGSkxFTkJRVU03VTBGRFlqdFJRVU5FTEVsQlFVa3NhVUpCUVdsQ0xFdEJRVXNzUzBGQlN5eEZRVUZGTzFsQlF5OUNMRTlCUVU4c1MwRkJTeXhEUVVGRE8xTkJRMlE3VVVGRFJDeFBRVUZQTEVsQlFVa3NRMEZCUXl4M1FrRkJkMElzUTBGQlF5eHBRa0ZCYVVJc1EwRkJReXhEUVVGRExGRkJRVkVzUTBGRE9VUXNXVUZCV1N4RFFVTmlMRU5CUVVNN1NVRkRTaXhEUVVGRE8wbEJSVThzYVVKQlFXbENMRU5CUVVNc1UwRkJVenRSUVVOcVF5eEpRVUZKTEVOQlFVTXNTVUZCU1N4RFFVRkRMR1ZCUVdVc1EwRkJReXhUUVVGVExFTkJRVU1zUlVGQlJUdFpRVU53UXl4SlFVRkpMRU5CUVVNc1pVRkJaU3hEUVVGRExGTkJRVk1zUTBGQlF5eEhRVUZITEVsQlFVa3NZMEZCWXl4RlFVRkZMRU5CUVVNN1UwRkRlRVE3VVVGRFJDeFBRVUZQTEVsQlFVa3NRMEZCUXl4bFFVRmxMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU03U1VGRGVrTXNRMEZCUXp0SlFVVlBMR3RDUVVGclFpeERRVUZETEZOQlFWTTdVVUZEYkVNc1NVRkJTU3hEUVVGRExFbEJRVWtzUTBGQlF5eG5Ra0ZCWjBJc1EwRkJReXhUUVVGVExFTkJRVU1zUlVGQlJUdFpRVU55UXl4SlFVRkpMRU5CUVVNc1owSkJRV2RDTEVOQlFVTXNVMEZCVXl4RFFVRkRMRWRCUVVjc1NVRkJTU3hsUVVGbExFVkJRVVVzUTBGQlF6dFRRVU14UkR0UlFVTkVMRTlCUVU4c1NVRkJTU3hEUVVGRExHZENRVUZuUWl4RFFVRkRMRk5CUVZNc1EwRkJReXhEUVVGRE8wbEJRekZETEVOQlFVTTdTVUZGUkRzN1QwRkZSenRKUVVWTExFdEJRVXNzUTBGQlF5d3dRa0ZCTUVJc1EwRkRkRU1zVDBGQmEwUXNSVUZEYkVRc1QwRkJUeXhGUVVOUUxGbEJRVzlDTzFGQlJYQkNMRTFCUVUwc1IwRkJSeXhIUVVOUUxFOUJRVThzUTBGQlF5eExRVUZMTEVkQlFVY3NRMEZCUXl4RFFVRkRPMWxCUTJoQ0xFTkJRVU1zUTBGQlF5eE5RVUZOTEU5QlFVOHNRMEZCUXl4SlFVRkpMRU5CUVVNc1IwRkJSeXhEUVVGRExFOUJRVThzUTBGQlF5eExRVUZMTEVOQlFVTTdXVUZEZGtNc1EwRkJReXhEUVVGRExFVkJRVVVzVVVGQlVTeEZRVUZGTEZOQlFWTXNSVUZCUlN4VFFVRlRMRVZCUVVVc1UwRkJVeXhGUVVGRkxFZEJRVWNzUlVGQlJTeFRRVUZUTEVWQlFVVXNRMEZCUXp0UlFVVndSU3hOUVVGTkxFMUJRVTBzUjBGQlJ5eEZRVUZwUWl4RFFVRkRPMUZCUldwRExFMUJRVTBzUTBGQlF5eFRRVUZUTEVkQlFVY3NVMEZCVXl4RFFVRkRMRWRCUVVjc1EwRkJReXhUUVVGVExFTkJRVU1zUTBGQlF6dFJRVU0xUXl4TlFVRk5MRU5CUVVNc1ZVRkJWU3hIUVVGSExFOUJRVThzUTBGQlF6dFJRVU0xUWl4TlFVRk5MRU5CUVVNc2MwSkJRWE5DTEVkQlFVY3NiMEpCUVc5Q0xFTkJRVU03VVVGRGNrUXNUVUZCVFN4RFFVRkRMR0ZCUVdFc1IwRkJSeXhaUVVGWkxFTkJRVU03VVVGRGNFTXNUVUZCVFN4RFFVRkRMRk5CUVZNc1IwRkJSeXhIUVVGSExFTkJRVU1zVVVGQlVTeERRVUZETzFGQlEyaERMRTFCUVUwc1EwRkJReXhOUVVGTkxFZEJRVWNzVDBGQlR5eERRVUZETEV0QlFVc3NRMEZCUXp0UlFVTTVRaXhOUVVGTkxFTkJRVU1zVVVGQlVTeEhRVUZITEU5QlFVOHNRMEZCUXl4UFFVRlBMRU5CUVVNN1VVRkZiRU1zYlVaQlFXMUdPMUZCUTI1R0xFMUJRVTBzUTBGQlF5eFZRVUZWTEVkQlFVY3NUVUZCVFN4RFFVRkRMRTlCUVU4c1EwRkJReXhUUVVGVExFTkJRVU1zUTBGQlF6dFJRVVU1UXl4TlFVRk5MRWRCUVVjc1IwRkJSeXhQUVVGUExFTkJRVU1zUjBGQlJ5eERRVUZETzFGQlEzaENMRTFCUVUwc1EwRkJReXhIUVVGSExFZEJRVWNzVTBGQlV5eERRVUZETEVkQlFVY3NRMEZCUXl4RFFVRkRPMUZCUlRWQ0xFMUJRVTBzWVVGQllTeEhRVUZITEU5QlFVOHNRMEZCUXl4TlFVRk5MRU5CUVVNN1VVRkRja01zVFVGQlRTeERRVUZETEUxQlFVMHNSMEZCUnl4WlFVRlpMRU5CUVVNc1lVRkJZU3hEUVVGRExFTkJRVU03VVVGRk5VTXNUVUZCVFN4WlFVRlpMRWRCUVVjc1NVRkJTU3hKUVVGSkxFTkJRVU1zVDBGQlR5eERRVUZETEZOQlFWTXNRMEZCUXl4RFFVRkRPMUZCUTJwRUxFMUJRVTBzUTBGQlF5eFZRVUZWTEVkQlFVY3NXVUZCV1N4RFFVRkRMRmRCUVZjc1JVRkJSU3hEUVVGRE8xRkJSUzlETEVsQlFVa3NXVUZCV1N4SFFVRkhMRVZCUVVVc1EwRkJRenRSUVVOMFFpeEpRVUZKTEZGQlFWRXNSMEZCUnl4RlFVRkZMRU5CUVVNN1VVRkRiRUlzVFVGQlRTeFBRVUZQTEVkQlFVY3NSVUZCUlN4RFFVRkRPMUZCUTI1Q0xFbEJRVWtzVFVGQlRTeEhRVUZITEV0QlFVc3NRMEZCUXp0UlFVTnVRaXhKUVVGSkxFOUJRVThzUTBGQlF5eGpRVUZqTEVWQlFVVTdXVUZETVVJc1QwRkJUeXhEUVVGRExHTkJRV01zUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXl4aFFVRmhMRVZCUVVVc1JVRkJSVHRuUWtGRE0wTXNUVUZCVFN4RlFVRkZMRWxCUVVrc1JVRkJSU3hMUVVGTExFVkJRVVVzUjBGQlJ5eGhRVUZoTEVOQlFVTTdaMEpCUTNSRExFMUJRVTBzVjBGQlZ5eEhRVUZITEVWQlFVVXNRMEZCUXp0blFrRkRka0lzVjBGQlZ5eERRVUZETEVsQlFVa3NRMEZCUXl4WlFVRlpMRU5CUVVNc1NVRkJTU3hEUVVGRExFTkJRVU1zUTBGQlF6dG5Ra0ZEY2tNc1YwRkJWeXhEUVVGRExFbEJRVWtzUTBGQlF5eFpRVUZaTEVOQlFVTXNTMEZCU3l4RFFVRkRMRU5CUVVNc1EwRkJRenRuUWtGRGRFTXNUMEZCVHl4RFFVRkRMRWxCUVVrc1EwRkJReXhYUVVGWExFTkJRVU1zUTBGQlF6dG5Ra0ZETVVJc1NVRkJTU3hKUVVGSkxFdEJRVXNzWTBGQll5eEZRVUZGTzI5Q1FVTXpRaXhaUVVGWkxFZEJRVWNzUzBGQlN5eERRVUZETzI5Q1FVTnlRaXhKUVVGSkxGbEJRVmtzUTBGQlF5eFBRVUZQTEVOQlFVTXNNRUpCUVRCQ0xFTkJRVU1zUzBGQlN5eERRVUZETEVOQlFVTXNSVUZCUlR0M1FrRkRNMFFzVFVGQlRTeEhRVUZITEVsQlFVa3NRMEZCUXp0eFFrRkRaanRwUWtGRFJqdG5Ra0ZEUkN4SlFVRkpMRWxCUVVrc1MwRkJTeXhUUVVGVExFVkJRVVU3YjBKQlEzUkNMRkZCUVZFc1IwRkJSeXhMUVVGTExFTkJRVU03YVVKQlEyeENPMWxCUTBnc1EwRkJReXhEUVVGRExFTkJRVU03VTBGRFNqdFJRVVZFTEUxQlFVMHNRMEZCUXl4UlFVRlJMRWRCUVVjc1dVRkJXU3hEUVVGRExGRkJRVkVzUTBGQlF5eERRVUZETzFGQlJYcERMRWxCUVVrc1lVRkJZU3hMUVVGTExFMUJRVTBzU1VGQlNTeERRVUZETEUxQlFVMHNRMEZCUXl4cFEwRkJhVU1zUlVGQlJUdFpRVU42UlN4TlFVRk5MR05CUVdNc1IwRkJSeXhKUVVGSkxFTkJRVU1zYVVKQlFXbENMRU5CUVVNc1QwRkJUeXhEUVVGRExGTkJRVk1zUTBGQlF5eERRVUZETzFsQlEycEZMRTFCUVUwc1VVRkJVU3hIUVVGSExFMUJRVTBzWTBGQll5eERRVUZETEhGQ1FVRnhRaXhEUVVGRExFbEJRVWtzUTBGQlF5eERRVUZETzFsQlEyeEZMRWxCUVVrc1EwRkJReXhSUVVGUkxFVkJRVVU3WjBKQlEySXNTVUZCU1N4RFFVRkRMRmxCUVZrc1EwRkJReXhSUVVGUkxFTkJRM2hDTEhGSFFVRnhSeXhEUVVOMFJ5eERRVUZETzJGQlEwZzdhVUpCUVUwN1owSkJRMHdzVFVGQlRTd3lRa0ZCTWtJc1IwRkRMMElzVFVGQlRTeGpRVUZqTEVOQlFVTXNNa0pCUVRKQ0xFTkJRVU03WjBKQlEyNUVMRTFCUVUwc1YwRkJWeXhIUVVGSExESkNRVUV5UWl4RFFVRkRMRmRCUVZjc1EwRkJRenRuUWtGRk5VUXNTVUZCU1N4WFFVRlhMRVZCUVVVN2IwSkJRMllzVFVGQlRTeFZRVUZWTEVkQlFVY3NTVUZCU1N4alFVRmpMRU5CUTI1RExESkNRVUV5UWl4RlFVTXpRaXhKUVVGSkxFTkJRVU1zV1VGQldTeERRVU5zUWl4RFFVRkRPMjlDUVVOR0xFMUJRVTBzVDBGQlR5eEhRVUZ6UWl4VlFVRlZMRU5CUVVNc1owSkJRV2RDTEVWQlFVVXNRMEZCUXp0dlFrRkZha1VzWjBSQlFXZEVPMjlDUVVOb1JDeEpRVUZKTEdOQlFXTXNTVUZCU1N4UFFVRlBMRVZCUVVVN2QwSkJRemRDTERCR1FVRXdSanQzUWtGRE1VWXNiVWRCUVcxSE8zZENRVU51Unl4TlFVRk5MR05CUVdNc1IwRkJSenMwUWtGRGNrSXNZMEZCWXpzMFFrRkRaQ3h4UWtGQmNVSTdORUpCUTNKQ0xHZENRVUZuUWp0NVFrRkRha0lzUTBGQlF6dDNRa0ZEUml4TFFVRkxMRTFCUVUwc1NVRkJTU3hKUVVGSkxFOUJRVThzUTBGQlF5eFpRVUZaTEVWQlFVVTdORUpCUTNaRExFbEJRVWtzWTBGQll5eERRVUZETEZGQlFWRXNRMEZCUXl4SlFVRkpMRU5CUVVNc1JVRkJSVHRuUTBGRGFrTXNUVUZCVFN4WFFVRlhMRWRCUVVjc1JVRkJSU3hEUVVGRE8yZERRVU4yUWl4WFFVRlhMRU5CUVVNc1NVRkJTU3hEUVVGRExGbEJRVmtzUTBGQlF5eEpRVUZKTEVOQlFVTXNRMEZCUXl4RFFVRkRPMmREUVVOeVF5eFhRVUZYTEVOQlFVTXNTVUZCU1N4RFFVRkRMRmxCUVZrc1EwRkJReXhQUVVGUExFTkJRVU1zV1VGQldTeERRVUZETEVsQlFVa3NRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJRenRuUTBGRE0wUXNUMEZCVHl4RFFVRkRMRWxCUVVrc1EwRkJReXhYUVVGWExFTkJRVU1zUTBGQlF6czJRa0ZETTBJN2VVSkJRMFk3Y1VKQlEwWTdiMEpCUTBRc0swWkJRU3RHTzI5Q1FVTXZSaXhKUVVGSkxGZEJRVmNzU1VGQlNTeFBRVUZQTEVWQlFVVTdkMEpCUXpGQ0xFMUJRVTBzUTBGQlF5eFRRVUZUTEVkQlFVY3NUMEZCVHl4RFFVRkRMRk5CUVZNc1EwRkJRenR4UWtGRGRFTTdiMEpCUTBRc1NVRkJTU3hsUVVGbExFbEJRVWtzVDBGQlR5eEZRVUZGTzNkQ1FVTTVRaXhOUVVGTkxFTkJRVU1zWVVGQllTeEhRVUZITEU5QlFVOHNRMEZCUXl4aFFVRmhMRU5CUVVNN2NVSkJRemxETzJsQ1FVTkdPMkZCUTBZN1UwRkRSanRSUVVWRUxFMUJRVTBzUTBGQlF5eFBRVUZQTEVkQlFVY3NTVUZCU1N4RFFVRkRMRk5CUVZNc1EwRkJReXhQUVVGUExFTkJRVU1zUTBGQlF6dFJRVVY2UXl4bFFVRmxPMUZCUTJZc1RVRkJUU3hMUVVGTExFZEJRVWNzVDBGQlR5eERRVUZETEVsQlFVa3NTMEZCU3l4blFrRkJaMElzUTBGQlF6dFJRVU5vUkN4TlFVRk5MRU5CUVVNc1RVRkJUU3hIUVVGSExGTkJRVk1zUTBGQlF5eExRVUZMTEVOQlFVTXNRMEZCUXp0UlFVVnFReXcyUTBGQk5rTTdVVUZETjBNc1NVRkJTU3huUWtGQlowSXNRMEZCUXp0UlFVTnlRaXhKUVVGSkxHRkJRV0VzUTBGQlF6dFJRVU5zUWl4SlFVRkpMRTlCUVU4c1EwRkJReXhUUVVGVExFVkJRVVU3V1VGRGNrSXNUVUZCVFN4bFFVRmxMRWRCUVVjc1NVRkJTU3hIUVVGSExFTkJRVU1zVDBGQlR5eERRVUZETEZOQlFWTXNRMEZCUXl4RFFVRkRPMWxCUTI1RUxHZENRVUZuUWl4SFFVRkhMR1ZCUVdVc1EwRkJReXhOUVVGTkxFTkJRVU03VTBGRE0wTTdVVUZEUkN4SlFVRkpMRTlCUVU4c1EwRkJReXhYUVVGWExFVkJRVVU3V1VGRGRrSXNUVUZCVFN4cFFrRkJhVUlzUjBGQlJ5eEpRVUZKTEVkQlFVY3NRMEZCUXl4UFFVRlBMRU5CUVVNc1YwRkJWeXhEUVVGRExFTkJRVU03V1VGRGRrUXNZVUZCWVN4SFFVRkhMR2xDUVVGcFFpeERRVUZETEUxQlFVMHNRMEZCUXp0VFFVTXhRenRSUVVORUxFMUJRVTBzUTBGQlF5eHBRa0ZCYVVJc1IwRkJSeXhaUVVGWkxFTkJRVU1zWjBKQlFXZENMRU5CUVVNc1EwRkJRenRSUVVNeFJDeE5RVUZOTEVOQlFVTXNZMEZCWXl4SFFVRkhMRmxCUVZrc1EwRkJReXhoUVVGaExFTkJRVU1zUTBGQlF6dFJRVVZ3UkN4NVFrRkJlVUk3VVVGRGVrSXNlVVZCUVhsRk8xRkJRM3BGTERoQ1FVRTRRanRSUVVNNVFpeE5RVUZOTEZkQlFWY3NSMEZCUnl4UFFVRlBMRU5CUVVNc1YwRkJWeXhEUVVGRE8xRkJRM2hETEUxQlFVMHNRMEZCUXl4WlFVRlpMRWRCUVVjc1dVRkJXU3hEUVVGRExGZEJRVmNzUTBGQlF5eERRVUZETzFGQlJXaEVMR3RGUVVGclJUdFJRVU5zUlN4cFJrRkJhVVk3VVVGRGFrWXNhVUpCUVdsQ08xRkJRMnBDTEhGSFFVRnhSenRSUVVOeVJ5eE5RVUZOTEVOQlFVTXNZVUZCWVN4SFFVRkhMRTlCUVU4c1EwRkJReXhKUVVGSkxFTkJRVU03VVVGRmNFTTdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3p0VlFUQkRSVHRSUVVOR0xFMUJRVTBzUTBGQlF5eGhRVUZoTEVkQlFVY3NVMEZCVXl4RFFVRkRMRWxCUVVrc1EwRkJReXgzUWtGQmQwSXNRMEZCUXl4UFFVRlBMRU5CUVVNc1EwRkJReXhEUVVGRE8xRkJRM3BGTEUxQlFVMHNRMEZCUXl4bFFVRmxMRWRCUVVjc1QwRkJUeXhEUVVGRExHRkJRV0VzUTBGQlF6dFJRVU12UXl4TlFVRk5MRU5CUVVNc1pVRkJaU3hIUVVGSExGbEJRVmtzUTBGRGJrTXNTVUZCU1N4RFFVRkRMRk5CUVZNc1EwRkJReXhQUVVGUExFTkJRVU1zWTBGQll5eERRVUZETEVOQlEzWkRMRU5CUVVNN1VVRkRSaXhKUVVGSkxFTkJRVU1zV1VGQldTeERRVUZETEZWQlFWVXNRMEZCUXl4bFFVRmxMRVZCUVVVc1RVRkJUU3hEUVVGRExFTkJRVU03U1VGRGVFUXNRMEZCUXp0SlFVVkVPenM3T3pzN096czdPenM3VDBGWlJ6dEpRVU5MTEhkQ1FVRjNRaXhEUVVNNVFpeFBRVUZyUkR0UlFVVnNSQ3hKUVVGSkxFZEJRVWNzUjBGQlJ5eEZRVUZGTEVOQlFVTTdVVUZGWWl4SlFVRkpMRTlCUVU4c1EwRkJReXhKUVVGSkxFdEJRVXNzV1VGQldTeEZRVUZGTzFsQlEycERMSGREUVVGM1F6dFpRVU40UXl4SFFVRkhMRWRCUVVjc1QwRkJUeXhEUVVGRExFZEJRVWNzUTBGQlF6dFRRVU51UWp0aFFVRk5MRWxCUVVrc1QwRkJUeXhEUVVGRExHTkJRV01zUTBGQlF5eG5Ra0ZCWjBJc1EwRkJReXhGUVVGRk8xbEJRMjVFTEdsRlFVRnBSVHRaUVVOcVJTeHpSVUZCYzBVN1dVRkRkRVVzUjBGQlJ5eEhRVUZITEU5QlFVOHNRMEZCUXl4alFVRmpMRU5CUVVNc1RVRkJUVHRuUWtGRGFrTXNRMEZCUXl4RFFVRkRMRTlCUVU4c1EwRkJReXhqUVVGakxFTkJRVU1zVDBGQlR5eERRVUZETEdOQlFXTXNRMEZCUXl4TlFVRk5MRWRCUVVjc1EwRkJReXhEUVVGRExFTkJRVU1zUjBGQlJ6dG5Ra0ZETDBRc1EwRkJReXhEUVVGRExFOUJRVThzUTBGQlF5eFhRVUZYTEVOQlFVTTdVMEZEZWtJN1lVRkJUVHRaUVVOTUxIVkVRVUYxUkR0WlFVTjJSQ3gzUmtGQmQwWTdXVUZEZUVZc1IwRkJSeXhIUVVGSExFOUJRVThzUTBGQlF5eFhRVUZYTEVOQlFVTTdVMEZETTBJN1VVRkRSQ3hQUVVGUExFZEJRVWNzUTBGQlF6dEpRVU5pTEVOQlFVTTdTVUZGVHl4TFFVRkxMRU5CUVVNc2RVSkJRWFZDTEVOQlEyNURMRTlCUVN0RExFVkJReTlETEU5QlFVOHNSVUZEVUN4WlFVRnZRanRSUVVWd1FqczdPenM3TzFWQlRVVTdVVUZGUml3MFFrRkJORUk3VVVGRE5VSXNhVVJCUVdsRU8xRkJSV3BFT3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096dFZRVEpFUlR0UlFVVkdMRTFCUVUwc1kwRkJZeXhIUVVGSExFOUJRVThzUTBGQlF5eFZRVUZWTEVOQlFVTTdVVUZETVVNc1RVRkJUU3hyUWtGQmEwSXNSMEZCUnl4UFFVRlBMRU5CUVVNc1ZVRkJWU3hEUVVGRE8xRkJSVGxETEUxQlFVMHNSMEZCUnl4SFFVTlFMRTlCUVU4c1EwRkJReXhMUVVGTExFZEJRVWNzUTBGQlF5eERRVUZETzFsQlEyaENMRU5CUVVNc1EwRkJReXhOUVVGTkxFOUJRVThzUTBGQlF5eEpRVUZKTEVOQlFVTXNSMEZCUnl4RFFVRkRMRTlCUVU4c1EwRkJReXhMUVVGTExFTkJRVU03V1VGRGRrTXNRMEZCUXl4RFFVRkRMRVZCUVVVc1VVRkJVU3hGUVVGRkxGTkJRVk1zUlVGQlJTeFRRVUZUTEVWQlFVVXNVMEZCVXl4RlFVRkZMRU5CUVVNN1VVRkRjRVFzVFVGQlRTeFpRVUZaTEVkQlFXbENPMWxCUTJwRExGTkJRVk1zUlVGQlJTeFRRVUZUTEVOQlFVTXNSMEZCUnl4RFFVRkRMRk5CUVZNc1EwRkJRenRaUVVOdVF5eFZRVUZWTEVWQlFVVXNUMEZCVHp0WlFVTnVRaXhsUVVGbExFVkJRVVVzVTBGQlV5eERRVUZETEU5QlFVOHNRMEZCUXl4SFFVRkhMRU5CUVVNN1dVRkRka01zWTBGQll5eEZRVUZGTEU5QlFVOHNRMEZCUXl4VFFVRlRPMWxCUTJwRExHVkJRV1VzUlVGQlJTeFRRVUZUTEVOQlFVTXNUMEZCVHl4RFFVRkRMRmRCUVZjc1EwRkJRenRaUVVNdlF5eGpRVUZqTEVWQlFVVXNTVUZCU1R0WlFVTndRaXh6UWtGQmMwSXNSVUZCUlN4dlFrRkJiMEk3V1VGRE5VTXNZVUZCWVN4RlFVRkZMRmxCUVZrN1dVRkRNMElzVTBGQlV5eEZRVUZGTEVkQlFVY3NRMEZCUXl4UlFVRlJPMWxCUTNaQ0xFMUJRVTBzUlVGQlJTeFBRVUZQTEVOQlFVTXNTMEZCU3p0WlFVTnlRaXhSUVVGUkxFVkJRVVVzVDBGQlR5eERRVUZETEU5QlFVODdXVUZEZWtJc1pVRkJaU3hGUVVGRkxHTkJRV003V1VGREwwSXNiMEpCUVc5Q0xFVkJRVVVzV1VGQldTeERRVUZETEd0Q1FVRnJRaXhEUVVGRE8xbEJRM1JFTEU5QlFVOHNSVUZCUlN4SlFVRkpMRU5CUVVNc1kwRkJZeXhEUVVGRExFOUJRVThzUTBGQlF5eGxRVUZsTEVOQlFVTXNRMEZCUXl4UFFVRlBPMWxCUXpkRUxGVkJRVlVzUlVGQlJTeEpRVUZKTEVsQlFVa3NRMEZCUXl4UFFVRlBMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU1zVjBGQlZ5eEZRVUZGTzFOQlEzUkVMRU5CUVVNN1VVRkZSaXhKUVVGSkxFTkJRVU1zV1VGQldTeERRVUZETEZWQlFWVXNRMEZCUXl4blFrRkJaMElzUlVGQlJTeFpRVUZaTEVOQlFVTXNRMEZCUXp0SlFVTXZSQ3hEUVVGRE8wbEJSVVE3TzA5QlJVYzdTVUZGU3l4TFFVRkxMRU5CUVVNc2JVSkJRVzFDTEVOQlF5OUNMRTlCUVRoRExFVkJRemxETEUxQlFXOUNPMUZCUlhCQ0xFMUJRVTBzWlVGQlpTeEhRVUZITEVsQlFVa3NRMEZCUXl4clFrRkJhMElzUTBGQlF5eFBRVUZQTEVOQlFVTXNVMEZCVXl4RFFVRkRMRU5CUVVNN1VVRkRia1VzU1VGQlNUdFpRVU5HTEUxQlFVMHNiMEpCUVc5Q0xFZEJRVWNzWlVGQlpTeERRVUZETEc5Q1FVRnZRaXhEUVVGRE8xbEJRMnhGTEUxQlFVMHNVVUZCVVN4SFFVRkhMRTFCUVUwc2IwSkJRVzlDTEVOQlFVTXNaVUZCWlN4RlFVRkZMRU5CUVVNN1dVRkRPVVFzVFVGQlRTeFhRVUZYTEVkQlFVY3NUVUZCVFN4dlFrRkJiMElzUTBGQlF5eGpRVUZqTEVWQlFVVXNRMEZCUXp0WlFVTm9SU3hKUVVGSkxFTkJRVU1zV1VGQldTeERRVUZETEZkQlFWY3NRMEZCUXl4UlFVRlJMRVZCUVVVc1dVRkJXU3hEUVVGRExGZEJRVmNzUTBGQlF5eERRVUZETEVOQlFVTTdXVUZEYmtVc1RVRkJUU3hEUVVGRExGbEJRVmtzUjBGQlJ5eFhRVUZYTEVOQlFVTTdXVUZEYkVNc1NVRkJTU3hEUVVGRExGbEJRVmtzUTBGQlF5eFZRVUZWTEVOQlFVTXNaMEpCUVdkQ0xFVkJRVVVzVFVGQlRTeERRVUZETEVOQlFVTTdVMEZEZUVRN1VVRkJReXhQUVVGUExFZEJRVWNzUlVGQlJUdFpRVU5hT3pzN096czdPMk5CVDBVN1dVRkRSaXhKUVVGSkxFTkJRVU1zV1VGQldTeERRVUZETEZGQlFWRXNRMEZEZUVJc2JVTkJRVzFETzJkQ1FVTnFReXh6UkVGQmMwUTdaMEpCUTNSRUxFZEJRVWNzUTBGQlF5eEpRVUZKTzJkQ1FVTlNMRWRCUVVjc1EwRkJReXhQUVVGUE8yZENRVU5ZTEVsQlFVazdaMEpCUTBvc1IwRkJSeXhEUVVGRExFdEJRVXNzUTBGRFdpeERRVUZETzFsQlEwWXNUVUZCVFN4RFFVRkRMRmxCUVZrc1IwRkJSeXhUUVVGVExFTkJRVU03V1VGRGFFTXNTVUZCU1N4RFFVRkRMRmxCUVZrc1EwRkJReXhWUVVGVkxFTkJRVU1zWjBKQlFXZENMRVZCUVVVc1RVRkJUU3hEUVVGRExFTkJRVU03VTBGRGVFUTdTVUZEU0N4RFFVRkRPMGxCUlVRc05FSkJRVFJDTzBsQlEzQkNMRXRCUVVzc1EwRkJReXhyUWtGQmEwSXNRMEZET1VJc1QwRkJNRU1zUlVGRE1VTXNUMEZCVHl4RlFVTlFMRmxCUVZrc1JVRkRXaXhYUVVGWE8xRkJSVmc3T3pzN096czdWVUZQUlR0UlFVVkdMRTFCUVUwc1IwRkJSeXhIUVVOUUxFOUJRVThzUTBGQlF5eExRVUZMTEVkQlFVY3NRMEZCUXl4RFFVRkRPMWxCUTJoQ0xFTkJRVU1zUTBGQlF5eE5RVUZOTEU5QlFVOHNRMEZCUXl4SlFVRkpMRU5CUVVNc1IwRkJSeXhEUVVGRExFOUJRVThzUTBGQlF5eExRVUZMTEVOQlFVTTdXVUZEZGtNc1EwRkJReXhEUVVGRExFVkJRVVVzVVVGQlVTeEZRVUZGTEZOQlFWTXNSVUZCUlN4VFFVRlRMRVZCUVVVc1UwRkJVeXhGUVVGRkxFTkJRVU03VVVGRmNFUXNUVUZCVFN4TlFVRk5MRWRCUVVjc1JVRkJhMElzUTBGQlF6dFJRVVZzUXl4TlFVRk5MRU5CUVVNc1UwRkJVeXhIUVVGSExGTkJRVk1zUTBGQlF5eEhRVUZITEVOQlFVTXNVMEZCVXl4RFFVRkRMRU5CUVVNN1VVRkROVU1zVFVGQlRTeERRVUZETEZWQlFWVXNSMEZCUnl4UFFVRlBMRU5CUVVNN1VVRkROVUlzVFVGQlRTeERRVUZETEhOQ1FVRnpRaXhIUVVGSExHOUNRVUZ2UWl4RFFVRkRPMUZCUTNKRUxFMUJRVTBzUTBGQlF5eGhRVUZoTEVkQlFVY3NXVUZCV1N4RFFVRkRPMUZCUTNCRExFMUJRVTBzUTBGQlF5eFRRVUZUTEVkQlFVY3NSMEZCUnl4RFFVRkRMRkZCUVZFc1EwRkJRenRSUVVOb1F5eE5RVUZOTEVOQlFVTXNUVUZCVFN4SFFVRkhMRTlCUVU4c1EwRkJReXhMUVVGTExFTkJRVU03VVVGRE9VSXNUVUZCVFN4RFFVRkRMRkZCUVZFc1IwRkJSeXhQUVVGUExFTkJRVU1zVDBGQlR5eERRVUZETzFGQlJXeERMRzFHUVVGdFJqdFJRVU51Uml4TlFVRk5MRU5CUVVNc1ZVRkJWU3hIUVVGSExFMUJRVTBzUTBGQlF5eFBRVUZQTEVOQlFVTXNVMEZCVXl4RFFVRkRMRU5CUVVNN1VVRkZPVU1zVFVGQlRTeFJRVUZSTEVkQlFVY3NUMEZCVHl4RFFVRkRMRk5CUVZNc1EwRkJRenRSUVVOdVF5eE5RVUZOTEVOQlFVTXNVMEZCVXl4SFFVRkhMRk5CUVZNc1EwRkJReXhSUVVGUkxFTkJRVU1zUTBGQlF6dFJRVVYyUXl4TlFVRk5MRWRCUVVjc1IwRkJSeXhQUVVGUExFTkJRVU1zUjBGQlJ5eERRVUZETzFGQlEzaENMRTFCUVUwc1EwRkJReXhIUVVGSExFZEJRVWNzVTBGQlV5eERRVUZETEVkQlFVY3NRMEZCUXl4RFFVRkRPMUZCUlRWQ0xFMUJRVTBzWVVGQllTeEhRVUZITEU5QlFVOHNRMEZCUXl4TlFVRk5MRU5CUVVNN1VVRkRja01zVFVGQlRTeERRVUZETEUxQlFVMHNSMEZCUnl4WlFVRlpMRU5CUVVNc1lVRkJZU3hEUVVGRExFTkJRVU03VVVGRk5VTXNNRVJCUVRCRU8xRkJRekZFTERaRlFVRTJSVHRSUVVNM1JTd3lSVUZCTWtVN1VVRkRNMFVzUlVGQlJUdFJRVU5HTEhGQ1FVRnhRanRSUVVOeVFpd3dRa0ZCTUVJN1VVRkRNVUlzYzBOQlFYTkRPMUZCUTNSRExFbEJRVWs3VVVGRFNpdzBRMEZCTkVNN1VVRkZOVU1zVFVGQlRTeGpRVUZqTEVkQlFVY3NUMEZCVHl4RFFVRkRMRlZCUVZVc1EwRkJRenRSUVVNeFF5eE5RVUZOTEVOQlFVTXNaVUZCWlN4SFFVRkhMR05CUVdNc1EwRkJRenRSUVVWNFF5eE5RVUZOTEd0Q1FVRnJRaXhIUVVGSExFOUJRVThzUTBGQlF5eFZRVUZWTEVOQlFVTTdVVUZET1VNc1RVRkJUU3hEUVVGRExHOUNRVUZ2UWl4SFFVRkhMRmxCUVZrc1EwRkJReXhyUWtGQmEwSXNRMEZCUXl4RFFVRkRPMUZCUlM5RUxFMUJRVTBzV1VGQldTeEhRVUZITEVsQlFVa3NTVUZCU1N4RFFVRkRMRTlCUVU4c1EwRkJReXhUUVVGVExFTkJRVU1zUTBGQlF6dFJRVU5xUkN4TlFVRk5MRU5CUVVNc1ZVRkJWU3hIUVVGSExGbEJRVmtzUTBGQlF5eFhRVUZYTEVWQlFVVXNRMEZCUXp0UlFVVXZReXhOUVVGTkxHRkJRV0VzUjBGQlJ5eEpRVUZKTEVOQlFVTXNZMEZCWXl4RFFVRkRMRTlCUVU4c1EwRkJReXhsUVVGbExFTkJRVU1zUTBGQlF6dFJRVU51UlN4TlFVRk5MRU5CUVVNc1QwRkJUeXhIUVVGSExHRkJRV0VzUTBGQlF5eFBRVUZQTEVOQlFVTTdVVUZEZGtNc1RVRkJUU3hEUVVGRExGRkJRVkVzUjBGQlJ5eGhRVUZoTEVOQlFVTXNVVUZCVVN4RFFVRkRPMUZCUlhwRExFbEJRVWtzU1VGQlNTeERRVUZETEdsQ1FVRnBRaXhEUVVGRExGZEJRVmNzUlVGQlJTeFBRVUZQTEVOQlFVTXNTVUZCU1N4RFFVRkRMRVZCUVVVN1dVRkRja1FzU1VGQlNTeERRVUZETEcxQ1FVRnRRaXhEUVVGRExFOUJRVThzUlVGQlJTeE5RVUZOTEVOQlFVTXNRMEZCUXp0VFFVTXpRenRoUVVGTk8xbEJRMHdzU1VGQlNTeERRVUZETEZsQlFWa3NRMEZCUXl4VlFVRlZMRU5CUVVNc1owSkJRV2RDTEVWQlFVVXNUVUZCVFN4RFFVRkRMRU5CUVVNN1UwRkRlRVE3U1VGRFNDeERRVUZETzBsQlJVOHNZMEZCWXl4RFFVRkRMRTlCUVc5Q08xRkJRM3BETEUxQlFVMHNZVUZCWVN4SFFVRkhMRVZCUVVVc1EwRkJRenRSUVVONlFpeEpRVUZKTEZGQlFWRXNSMEZCUnl4RlFVRkZMRU5CUVVNN1VVRkRiRUlzU1VGQlNTeFBRVUZQTEVWQlFVVTdXVUZEV0N4UFFVRlBMRU5CUVVNc1IwRkJSeXhEUVVGRExFTkJRVU1zWTBGQll5eEZRVUZGTEVWQlFVVTdaMEpCUXpkQ0xFMUJRVTBzUlVGQlJTeEpRVUZKTEVWQlFVVXNTMEZCU3l4RlFVRkZMRWRCUVVjc1kwRkJZeXhEUVVGRE8yZENRVU4yUXl4TlFVRk5MRmRCUVZjc1IwRkJSeXhGUVVGRkxFTkJRVU03WjBKQlEzWkNMRmRCUVZjc1EwRkJReXhKUVVGSkxFTkJRVU1zV1VGQldTeERRVUZETEVsQlFVa3NRMEZCUXl4RFFVRkRMRU5CUVVNN1owSkJRM0pETEZkQlFWY3NRMEZCUXl4SlFVRkpMRU5CUVVNc1dVRkJXU3hEUVVGRExFdEJRVXNzUTBGQlF5eERRVUZETEVOQlFVTTdaMEpCUTNSRExHRkJRV0VzUTBGQlF5eEpRVUZKTEVOQlFVTXNWMEZCVnl4RFFVRkRMRU5CUVVNN1owSkJRMmhETEVsQlFVa3NTVUZCU1N4RFFVRkRMRmRCUVZjc1JVRkJSU3hMUVVGTExGVkJRVlVzUlVGQlJUdHZRa0ZEY2tNc1VVRkJVU3hIUVVGSExFdEJRVXNzUTBGQlF6dHBRa0ZEYkVJN1dVRkRTQ3hEUVVGRExFTkJRVU1zUTBGQlF6dFRRVU5LTzFGQlEwUXNUMEZCVHp0WlFVTk1MRTlCUVU4c1JVRkJSU3hKUVVGSkxFTkJRVU1zVTBGQlV5eERRVUZETEdGQlFXRXNRMEZCUXp0WlFVTjBReXhSUVVGUkxFVkJRVVVzV1VGQldTeERRVUZETEZGQlFWRXNRMEZCUXp0VFFVTnFReXhEUVVGRE8wbEJRMG9zUTBGQlF6dERRVU5HSW4wPSIsImltcG9ydCB7IGluY3JlbWVudGVkRXZlbnRPcmRpbmFsIH0gZnJvbSBcIi4uL2xpYi9leHRlbnNpb24tc2Vzc2lvbi1ldmVudC1vcmRpbmFsXCI7XG5pbXBvcnQgeyBleHRlbnNpb25TZXNzaW9uVXVpZCB9IGZyb20gXCIuLi9saWIvZXh0ZW5zaW9uLXNlc3Npb24tdXVpZFwiO1xuaW1wb3J0IHsgYm9vbFRvSW50LCBlc2NhcGVTdHJpbmcsIGVzY2FwZVVybCB9IGZyb20gXCIuLi9saWIvc3RyaW5nLXV0aWxzXCI7XG5leHBvcnQgY2xhc3MgSmF2YXNjcmlwdEluc3RydW1lbnQge1xuICAgIC8qKlxuICAgICAqIENvbnZlcnRzIHJlY2VpdmVkIGNhbGwgYW5kIHZhbHVlcyBkYXRhIGZyb20gdGhlIEpTIEluc3RydW1lbnRhdGlvblxuICAgICAqIGludG8gdGhlIGZvcm1hdCB0aGF0IHRoZSBzY2hlbWEgZXhwZWN0cy5cbiAgICAgKlxuICAgICAqIEBwYXJhbSBkYXRhXG4gICAgICogQHBhcmFtIHNlbmRlclxuICAgICAqL1xuICAgIHN0YXRpYyBwcm9jZXNzQ2FsbHNBbmRWYWx1ZXMoZGF0YSwgc2VuZGVyKSB7XG4gICAgICAgIGNvbnN0IHVwZGF0ZSA9IHt9O1xuICAgICAgICB1cGRhdGUuZXh0ZW5zaW9uX3Nlc3Npb25fdXVpZCA9IGV4dGVuc2lvblNlc3Npb25VdWlkO1xuICAgICAgICB1cGRhdGUuZXZlbnRfb3JkaW5hbCA9IGluY3JlbWVudGVkRXZlbnRPcmRpbmFsKCk7XG4gICAgICAgIHVwZGF0ZS5wYWdlX3Njb3BlZF9ldmVudF9vcmRpbmFsID0gZGF0YS5vcmRpbmFsO1xuICAgICAgICB1cGRhdGUud2luZG93X2lkID0gc2VuZGVyLnRhYi53aW5kb3dJZDtcbiAgICAgICAgdXBkYXRlLnRhYl9pZCA9IHNlbmRlci50YWIuaWQ7XG4gICAgICAgIHVwZGF0ZS5mcmFtZV9pZCA9IHNlbmRlci5mcmFtZUlkO1xuICAgICAgICB1cGRhdGUuc2NyaXB0X3VybCA9IGVzY2FwZVVybChkYXRhLnNjcmlwdFVybCk7XG4gICAgICAgIHVwZGF0ZS5zY3JpcHRfbGluZSA9IGVzY2FwZVN0cmluZyhkYXRhLnNjcmlwdExpbmUpO1xuICAgICAgICB1cGRhdGUuc2NyaXB0X2NvbCA9IGVzY2FwZVN0cmluZyhkYXRhLnNjcmlwdENvbCk7XG4gICAgICAgIHVwZGF0ZS5mdW5jX25hbWUgPSBlc2NhcGVTdHJpbmcoZGF0YS5mdW5jTmFtZSk7XG4gICAgICAgIHVwZGF0ZS5zY3JpcHRfbG9jX2V2YWwgPSBlc2NhcGVTdHJpbmcoZGF0YS5zY3JpcHRMb2NFdmFsKTtcbiAgICAgICAgdXBkYXRlLmNhbGxfc3RhY2sgPSBlc2NhcGVTdHJpbmcoZGF0YS5jYWxsU3RhY2spO1xuICAgICAgICB1cGRhdGUuc3ltYm9sID0gZXNjYXBlU3RyaW5nKGRhdGEuc3ltYm9sKTtcbiAgICAgICAgdXBkYXRlLm9wZXJhdGlvbiA9IGVzY2FwZVN0cmluZyhkYXRhLm9wZXJhdGlvbik7XG4gICAgICAgIHVwZGF0ZS52YWx1ZSA9IGVzY2FwZVN0cmluZyhkYXRhLnZhbHVlKTtcbiAgICAgICAgdXBkYXRlLmF0dHJpYnV0ZXMgPSBlc2NhcGVTdHJpbmcoZGF0YS5hdHRyaWJ1dGVzKTtcbiAgICAgICAgdXBkYXRlLnRpbWVfc3RhbXAgPSBkYXRhLnRpbWVTdGFtcDtcbiAgICAgICAgdXBkYXRlLmluY29nbml0byA9IGJvb2xUb0ludChzZW5kZXIudGFiLmluY29nbml0byk7XG4gICAgICAgIC8vIGRvY3VtZW50X3VybCBpcyB0aGUgY3VycmVudCBmcmFtZSdzIGRvY3VtZW50IGhyZWZcbiAgICAgICAgLy8gdG9wX2xldmVsX3VybCBpcyB0aGUgdG9wLWxldmVsIGZyYW1lJ3MgZG9jdW1lbnQgaHJlZlxuICAgICAgICB1cGRhdGUuZG9jdW1lbnRfdXJsID0gZXNjYXBlVXJsKHNlbmRlci51cmwpO1xuICAgICAgICB1cGRhdGUudG9wX2xldmVsX3VybCA9IGVzY2FwZVVybChzZW5kZXIudGFiLnVybCk7XG4gICAgICAgIGlmIChkYXRhLm9wZXJhdGlvbiA9PT0gXCJjYWxsXCIgJiYgZGF0YS5hcmdzLmxlbmd0aCA+IDApIHtcbiAgICAgICAgICAgIHVwZGF0ZS5hcmd1bWVudHMgPSBlc2NhcGVTdHJpbmcoSlNPTi5zdHJpbmdpZnkoZGF0YS5hcmdzKSk7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHVwZGF0ZTtcbiAgICB9XG4gICAgZGF0YVJlY2VpdmVyO1xuICAgIG9uTWVzc2FnZUxpc3RlbmVyO1xuICAgIGNvbmZpZ3VyZWQgPSBmYWxzZTtcbiAgICBwZW5kaW5nUmVjb3JkcyA9IFtdO1xuICAgIGNyYXdsSUQ7XG4gICAgY29uc3RydWN0b3IoZGF0YVJlY2VpdmVyKSB7XG4gICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyID0gZGF0YVJlY2VpdmVyO1xuICAgIH1cbiAgICAvKipcbiAgICAgKiBTdGFydCBsaXN0ZW5pbmcgZm9yIG1lc3NhZ2VzIGZyb20gcGFnZS9jb250ZW50L2JhY2tncm91bmQgc2NyaXB0cyBpbmplY3RlZCB0byBpbnN0cnVtZW50IEphdmFTY3JpcHQgQVBJc1xuICAgICAqL1xuICAgIGxpc3RlbigpIHtcbiAgICAgICAgdGhpcy5vbk1lc3NhZ2VMaXN0ZW5lciA9IChtZXNzYWdlLCBzZW5kZXIpID0+IHtcbiAgICAgICAgICAgIGlmIChtZXNzYWdlLm5hbWVzcGFjZSAmJlxuICAgICAgICAgICAgICAgIG1lc3NhZ2UubmFtZXNwYWNlID09PSBcImphdmFzY3JpcHQtaW5zdHJ1bWVudGF0aW9uXCIpIHtcbiAgICAgICAgICAgICAgICB0aGlzLmhhbmRsZUpzSW5zdHJ1bWVudGF0aW9uTWVzc2FnZShtZXNzYWdlLCBzZW5kZXIpO1xuICAgICAgICAgICAgfVxuICAgICAgICB9O1xuICAgICAgICBicm93c2VyLnJ1bnRpbWUub25NZXNzYWdlLmFkZExpc3RlbmVyKHRoaXMub25NZXNzYWdlTGlzdGVuZXIpO1xuICAgIH1cbiAgICAvKipcbiAgICAgKiBFaXRoZXIgc2VuZHMgdGhlIGxvZyBkYXRhIHRvIHRoZSBkYXRhUmVjZWl2ZXIgb3Igc3RvcmUgaXQgaW4gbWVtb3J5XG4gICAgICogYXMgYSBwZW5kaW5nIHJlY29yZCBpZiB0aGUgSlMgaW5zdHJ1bWVudGF0aW9uIGlzIG5vdCB5ZXQgY29uZmlndXJlZFxuICAgICAqXG4gICAgICogQHBhcmFtIG1lc3NhZ2VcbiAgICAgKiBAcGFyYW0gc2VuZGVyXG4gICAgICovXG4gICAgaGFuZGxlSnNJbnN0cnVtZW50YXRpb25NZXNzYWdlKG1lc3NhZ2UsIHNlbmRlcikge1xuICAgICAgICBzd2l0Y2ggKG1lc3NhZ2UudHlwZSkge1xuICAgICAgICAgICAgY2FzZSBcImxvZ0NhbGxcIjpcbiAgICAgICAgICAgIGNhc2UgXCJsb2dWYWx1ZVwiOlxuICAgICAgICAgICAgICAgIGNvbnN0IHVwZGF0ZSA9IEphdmFzY3JpcHRJbnN0cnVtZW50LnByb2Nlc3NDYWxsc0FuZFZhbHVlcyhtZXNzYWdlLmRhdGEsIHNlbmRlcik7XG4gICAgICAgICAgICAgICAgaWYgKHRoaXMuY29uZmlndXJlZCkge1xuICAgICAgICAgICAgICAgICAgICB1cGRhdGUuYnJvd3Nlcl9pZCA9IHRoaXMuY3Jhd2xJRDtcbiAgICAgICAgICAgICAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIuc2F2ZVJlY29yZChcImphdmFzY3JpcHRcIiwgdXBkYXRlKTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgIHRoaXMucGVuZGluZ1JlY29yZHMucHVzaCh1cGRhdGUpO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBicmVhaztcbiAgICAgICAgfVxuICAgIH1cbiAgICAvKipcbiAgICAgKiBTdGFydHMgbGlzdGVuaW5nIGlmIGhhdmVuJ3QgZG9uZSBzbyBhbHJlYWR5LCBzZXRzIHRoZSBjcmF3bCBJRCxcbiAgICAgKiBtYXJrcyB0aGUgSlMgaW5zdHJ1bWVudGF0aW9uIGFzIGNvbmZpZ3VyZWQgYW5kIHNlbmRzIGFueSBwZW5kaW5nXG4gICAgICogcmVjb3JkcyB0aGF0IGhhdmUgYmVlbiByZWNlaXZlZCB1cCB1bnRpbCB0aGlzIHBvaW50LlxuICAgICAqXG4gICAgICogQHBhcmFtIGNyYXdsSURcbiAgICAgKi9cbiAgICBydW4oY3Jhd2xJRCkge1xuICAgICAgICBpZiAoIXRoaXMub25NZXNzYWdlTGlzdGVuZXIpIHtcbiAgICAgICAgICAgIHRoaXMubGlzdGVuKCk7XG4gICAgICAgIH1cbiAgICAgICAgdGhpcy5jcmF3bElEID0gY3Jhd2xJRDtcbiAgICAgICAgdGhpcy5jb25maWd1cmVkID0gdHJ1ZTtcbiAgICAgICAgdGhpcy5wZW5kaW5nUmVjb3Jkcy5tYXAoKHVwZGF0ZSkgPT4ge1xuICAgICAgICAgICAgdXBkYXRlLmJyb3dzZXJfaWQgPSB0aGlzLmNyYXdsSUQ7XG4gICAgICAgICAgICB0aGlzLmRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwiamF2YXNjcmlwdFwiLCB1cGRhdGUpO1xuICAgICAgICB9KTtcbiAgICB9XG4gICAgYXN5bmMgcmVnaXN0ZXJDb250ZW50U2NyaXB0KHRlc3RpbmcsIGpzSW5zdHJ1bWVudGF0aW9uU2V0dGluZ3MpIHtcbiAgICAgICAgY29uc3QgY29udGVudFNjcmlwdENvbmZpZyA9IHtcbiAgICAgICAgICAgIHRlc3RpbmcsXG4gICAgICAgICAgICBqc0luc3RydW1lbnRhdGlvblNldHRpbmdzLFxuICAgICAgICB9O1xuICAgICAgICBpZiAoY29udGVudFNjcmlwdENvbmZpZykge1xuICAgICAgICAgICAgLy8gVE9ETzogQXZvaWQgdXNpbmcgd2luZG93IHRvIHBhc3MgdGhlIGNvbnRlbnQgc2NyaXB0IGNvbmZpZ1xuICAgICAgICAgICAgYXdhaXQgYnJvd3Nlci5jb250ZW50U2NyaXB0cy5yZWdpc3Rlcih7XG4gICAgICAgICAgICAgICAganM6IFtcbiAgICAgICAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgICAgICAgICAgY29kZTogYHdpbmRvdy5vcGVuV3BtQ29udGVudFNjcmlwdENvbmZpZyA9ICR7SlNPTi5zdHJpbmdpZnkoY29udGVudFNjcmlwdENvbmZpZyl9O2AsXG4gICAgICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgXSxcbiAgICAgICAgICAgICAgICBtYXRjaGVzOiBbXCI8YWxsX3VybHM+XCJdLFxuICAgICAgICAgICAgICAgIGFsbEZyYW1lczogdHJ1ZSxcbiAgICAgICAgICAgICAgICBydW5BdDogXCJkb2N1bWVudF9zdGFydFwiLFxuICAgICAgICAgICAgICAgIG1hdGNoQWJvdXRCbGFuazogdHJ1ZSxcbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBicm93c2VyLmNvbnRlbnRTY3JpcHRzLnJlZ2lzdGVyKHtcbiAgICAgICAgICAgIGpzOiBbeyBmaWxlOiBcIi9jb250ZW50LmpzXCIgfV0sXG4gICAgICAgICAgICBtYXRjaGVzOiBbXCI8YWxsX3VybHM+XCJdLFxuICAgICAgICAgICAgYWxsRnJhbWVzOiB0cnVlLFxuICAgICAgICAgICAgcnVuQXQ6IFwiZG9jdW1lbnRfc3RhcnRcIixcbiAgICAgICAgICAgIG1hdGNoQWJvdXRCbGFuazogdHJ1ZSxcbiAgICAgICAgfSk7XG4gICAgfVxuICAgIGNsZWFudXAoKSB7XG4gICAgICAgIHRoaXMucGVuZGluZ1JlY29yZHMgPSBbXTtcbiAgICAgICAgaWYgKHRoaXMub25NZXNzYWdlTGlzdGVuZXIpIHtcbiAgICAgICAgICAgIGJyb3dzZXIucnVudGltZS5vbk1lc3NhZ2UucmVtb3ZlTGlzdGVuZXIodGhpcy5vbk1lc3NhZ2VMaXN0ZW5lcik7XG4gICAgICAgIH1cbiAgICB9XG59XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2lhbUYyWVhOamNtbHdkQzFwYm5OMGNuVnRaVzUwTG1weklpd2ljMjkxY21ObFVtOXZkQ0k2SWlJc0luTnZkWEpqWlhNaU9sc2lMaTR2TGk0dkxpNHZjM0pqTDJKaFkydG5jbTkxYm1RdmFtRjJZWE5qY21sd2RDMXBibk4wY25WdFpXNTBMblJ6SWwwc0ltNWhiV1Z6SWpwYlhTd2liV0Z3Y0dsdVozTWlPaUpCUVVOQkxFOUJRVThzUlVGQlJTeDFRa0ZCZFVJc1JVRkJSU3hOUVVGTkxIZERRVUYzUXl4RFFVRkRPMEZCUTJwR0xFOUJRVThzUlVGQlJTeHZRa0ZCYjBJc1JVRkJSU3hOUVVGTkxDdENRVUVyUWl4RFFVRkRPMEZCUTNKRkxFOUJRVThzUlVGQlJTeFRRVUZUTEVWQlFVVXNXVUZCV1N4RlFVRkZMRk5CUVZNc1JVRkJSU3hOUVVGTkxIRkNRVUZ4UWl4RFFVRkRPMEZCU1hwRkxFMUJRVTBzVDBGQlR5eHZRa0ZCYjBJN1NVRkRMMEk3T3pzN096dFBRVTFITzBsQlEwc3NUVUZCVFN4RFFVRkRMSEZDUVVGeFFpeERRVUZETEVsQlFVa3NSVUZCUlN4TlFVRnhRanRSUVVNNVJDeE5RVUZOTEUxQlFVMHNSMEZCUnl4RlFVRjVRaXhEUVVGRE8xRkJRM3BETEUxQlFVMHNRMEZCUXl4elFrRkJjMElzUjBGQlJ5eHZRa0ZCYjBJc1EwRkJRenRSUVVOeVJDeE5RVUZOTEVOQlFVTXNZVUZCWVN4SFFVRkhMSFZDUVVGMVFpeEZRVUZGTEVOQlFVTTdVVUZEYWtRc1RVRkJUU3hEUVVGRExIbENRVUY1UWl4SFFVRkhMRWxCUVVrc1EwRkJReXhQUVVGUExFTkJRVU03VVVGRGFFUXNUVUZCVFN4RFFVRkRMRk5CUVZNc1IwRkJSeXhOUVVGTkxFTkJRVU1zUjBGQlJ5eERRVUZETEZGQlFWRXNRMEZCUXp0UlFVTjJReXhOUVVGTkxFTkJRVU1zVFVGQlRTeEhRVUZITEUxQlFVMHNRMEZCUXl4SFFVRkhMRU5CUVVNc1JVRkJSU3hEUVVGRE8xRkJRemxDTEUxQlFVMHNRMEZCUXl4UlFVRlJMRWRCUVVjc1RVRkJUU3hEUVVGRExFOUJRVThzUTBGQlF6dFJRVU5xUXl4TlFVRk5MRU5CUVVNc1ZVRkJWU3hIUVVGSExGTkJRVk1zUTBGQlF5eEpRVUZKTEVOQlFVTXNVMEZCVXl4RFFVRkRMRU5CUVVNN1VVRkRPVU1zVFVGQlRTeERRVUZETEZkQlFWY3NSMEZCUnl4WlFVRlpMRU5CUVVNc1NVRkJTU3hEUVVGRExGVkJRVlVzUTBGQlF5eERRVUZETzFGQlEyNUVMRTFCUVUwc1EwRkJReXhWUVVGVkxFZEJRVWNzV1VGQldTeERRVUZETEVsQlFVa3NRMEZCUXl4VFFVRlRMRU5CUVVNc1EwRkJRenRSUVVOcVJDeE5RVUZOTEVOQlFVTXNVMEZCVXl4SFFVRkhMRmxCUVZrc1EwRkJReXhKUVVGSkxFTkJRVU1zVVVGQlVTeERRVUZETEVOQlFVTTdVVUZETDBNc1RVRkJUU3hEUVVGRExHVkJRV1VzUjBGQlJ5eFpRVUZaTEVOQlFVTXNTVUZCU1N4RFFVRkRMR0ZCUVdFc1EwRkJReXhEUVVGRE8xRkJRekZFTEUxQlFVMHNRMEZCUXl4VlFVRlZMRWRCUVVjc1dVRkJXU3hEUVVGRExFbEJRVWtzUTBGQlF5eFRRVUZUTEVOQlFVTXNRMEZCUXp0UlFVTnFSQ3hOUVVGTkxFTkJRVU1zVFVGQlRTeEhRVUZITEZsQlFWa3NRMEZCUXl4SlFVRkpMRU5CUVVNc1RVRkJUU3hEUVVGRExFTkJRVU03VVVGRE1VTXNUVUZCVFN4RFFVRkRMRk5CUVZNc1IwRkJSeXhaUVVGWkxFTkJRVU1zU1VGQlNTeERRVUZETEZOQlFWTXNRMEZCUXl4RFFVRkRPMUZCUTJoRUxFMUJRVTBzUTBGQlF5eExRVUZMTEVkQlFVY3NXVUZCV1N4RFFVRkRMRWxCUVVrc1EwRkJReXhMUVVGTExFTkJRVU1zUTBGQlF6dFJRVU40UXl4TlFVRk5MRU5CUVVNc1ZVRkJWU3hIUVVGSExGbEJRVmtzUTBGQlF5eEpRVUZKTEVOQlFVTXNWVUZCVlN4RFFVRkRMRU5CUVVNN1VVRkRiRVFzVFVGQlRTeERRVUZETEZWQlFWVXNSMEZCUnl4SlFVRkpMRU5CUVVNc1UwRkJVeXhEUVVGRE8xRkJRMjVETEUxQlFVMHNRMEZCUXl4VFFVRlRMRWRCUVVjc1UwRkJVeXhEUVVGRExFMUJRVTBzUTBGQlF5eEhRVUZITEVOQlFVTXNVMEZCVXl4RFFVRkRMRU5CUVVNN1VVRkZia1FzYjBSQlFXOUVPMUZCUTNCRUxIVkVRVUYxUkR0UlFVTjJSQ3hOUVVGTkxFTkJRVU1zV1VGQldTeEhRVUZITEZOQlFWTXNRMEZCUXl4TlFVRk5MRU5CUVVNc1IwRkJSeXhEUVVGRExFTkJRVU03VVVGRE5VTXNUVUZCVFN4RFFVRkRMR0ZCUVdFc1IwRkJSeXhUUVVGVExFTkJRVU1zVFVGQlRTeERRVUZETEVkQlFVY3NRMEZCUXl4SFFVRkhMRU5CUVVNc1EwRkJRenRSUVVWcVJDeEpRVUZKTEVsQlFVa3NRMEZCUXl4VFFVRlRMRXRCUVVzc1RVRkJUU3hKUVVGSkxFbEJRVWtzUTBGQlF5eEpRVUZKTEVOQlFVTXNUVUZCVFN4SFFVRkhMRU5CUVVNc1JVRkJSVHRaUVVOeVJDeE5RVUZOTEVOQlFVTXNVMEZCVXl4SFFVRkhMRmxCUVZrc1EwRkJReXhKUVVGSkxFTkJRVU1zVTBGQlV5eERRVUZETEVsQlFVa3NRMEZCUXl4SlFVRkpMRU5CUVVNc1EwRkJReXhEUVVGRE8xTkJRelZFTzFGQlJVUXNUMEZCVHl4TlFVRk5MRU5CUVVNN1NVRkRhRUlzUTBGQlF6dEpRVU5uUWl4WlFVRlpMRU5CUVVNN1NVRkRkRUlzYVVKQlFXbENMRU5CUVVNN1NVRkRiRUlzVlVGQlZTeEhRVUZaTEV0QlFVc3NRMEZCUXp0SlFVTTFRaXhqUVVGakxFZEJRVEJDTEVWQlFVVXNRMEZCUXp0SlFVTXpReXhQUVVGUExFTkJRVU03U1VGRmFFSXNXVUZCV1N4WlFVRlpPMUZCUTNSQ0xFbEJRVWtzUTBGQlF5eFpRVUZaTEVkQlFVY3NXVUZCV1N4RFFVRkRPMGxCUTI1RExFTkJRVU03U1VGRlJEczdUMEZGUnp0SlFVTkpMRTFCUVUwN1VVRkRXQ3hKUVVGSkxFTkJRVU1zYVVKQlFXbENMRWRCUVVjc1EwRkJReXhQUVVGUExFVkJRVVVzVFVGQlRTeEZRVUZGTEVWQlFVVTdXVUZETTBNc1NVRkRSU3hQUVVGUExFTkJRVU1zVTBGQlV6dG5Ra0ZEYWtJc1QwRkJUeXhEUVVGRExGTkJRVk1zUzBGQlN5dzBRa0ZCTkVJc1JVRkRiRVE3WjBKQlEwRXNTVUZCU1N4RFFVRkRMRGhDUVVFNFFpeERRVUZETEU5QlFVOHNSVUZCUlN4TlFVRk5MRU5CUVVNc1EwRkJRenRoUVVOMFJEdFJRVU5JTEVOQlFVTXNRMEZCUXp0UlFVTkdMRTlCUVU4c1EwRkJReXhQUVVGUExFTkJRVU1zVTBGQlV5eERRVUZETEZkQlFWY3NRMEZCUXl4SlFVRkpMRU5CUVVNc2FVSkJRV2xDTEVOQlFVTXNRMEZCUXp0SlFVTm9SU3hEUVVGRE8wbEJSVVE3T3pzN096dFBRVTFITzBsQlEwa3NPRUpCUVRoQ0xFTkJRVU1zVDBGQlR5eEZRVUZGTEUxQlFYRkNPMUZCUTJ4RkxGRkJRVkVzVDBGQlR5eERRVUZETEVsQlFVa3NSVUZCUlR0WlFVTndRaXhMUVVGTExGTkJRVk1zUTBGQlF6dFpRVU5tTEV0QlFVc3NWVUZCVlR0blFrRkRZaXhOUVVGTkxFMUJRVTBzUjBGQlJ5eHZRa0ZCYjBJc1EwRkJReXh4UWtGQmNVSXNRMEZEZGtRc1QwRkJUeXhEUVVGRExFbEJRVWtzUlVGRFdpeE5RVUZOTEVOQlExQXNRMEZCUXp0blFrRkRSaXhKUVVGSkxFbEJRVWtzUTBGQlF5eFZRVUZWTEVWQlFVVTdiMEpCUTI1Q0xFMUJRVTBzUTBGQlF5eFZRVUZWTEVkQlFVY3NTVUZCU1N4RFFVRkRMRTlCUVU4c1EwRkJRenR2UWtGRGFrTXNTVUZCU1N4RFFVRkRMRmxCUVZrc1EwRkJReXhWUVVGVkxFTkJRVU1zV1VGQldTeEZRVUZGTEUxQlFVMHNRMEZCUXl4RFFVRkRPMmxDUVVOd1JEdHhRa0ZCVFR0dlFrRkRUQ3hKUVVGSkxFTkJRVU1zWTBGQll5eERRVUZETEVsQlFVa3NRMEZCUXl4TlFVRk5MRU5CUVVNc1EwRkJRenRwUWtGRGJFTTdaMEpCUTBRc1RVRkJUVHRUUVVOVU8wbEJRMGdzUTBGQlF6dEpRVVZFT3pzN096czdUMEZOUnp0SlFVTkpMRWRCUVVjc1EwRkJReXhQUVVGUE8xRkJRMmhDTEVsQlFVa3NRMEZCUXl4SlFVRkpMRU5CUVVNc2FVSkJRV2xDTEVWQlFVVTdXVUZETTBJc1NVRkJTU3hEUVVGRExFMUJRVTBzUlVGQlJTeERRVUZETzFOQlEyWTdVVUZEUkN4SlFVRkpMRU5CUVVNc1QwRkJUeXhIUVVGSExFOUJRVThzUTBGQlF6dFJRVU4yUWl4SlFVRkpMRU5CUVVNc1ZVRkJWU3hIUVVGSExFbEJRVWtzUTBGQlF6dFJRVU4yUWl4SlFVRkpMRU5CUVVNc1kwRkJZeXhEUVVGRExFZEJRVWNzUTBGQlF5eERRVUZETEUxQlFVMHNSVUZCUlN4RlFVRkZPMWxCUTJwRExFMUJRVTBzUTBGQlF5eFZRVUZWTEVkQlFVY3NTVUZCU1N4RFFVRkRMRTlCUVU4c1EwRkJRenRaUVVOcVF5eEpRVUZKTEVOQlFVTXNXVUZCV1N4RFFVRkRMRlZCUVZVc1EwRkJReXhaUVVGWkxFVkJRVVVzVFVGQlRTeERRVUZETEVOQlFVTTdVVUZEY2tRc1EwRkJReXhEUVVGRExFTkJRVU03U1VGRFRDeERRVUZETzBsQlJVMHNTMEZCU3l4RFFVRkRMSEZDUVVGeFFpeERRVU5vUXl4UFFVRm5RaXhGUVVOb1FpeDVRa0ZCWjBRN1VVRkZhRVFzVFVGQlRTeHRRa0ZCYlVJc1IwRkJSenRaUVVNeFFpeFBRVUZQTzFsQlExQXNlVUpCUVhsQ08xTkJRekZDTEVOQlFVTTdVVUZEUml4SlFVRkpMRzFDUVVGdFFpeEZRVUZGTzFsQlEzWkNMRFpFUVVFMlJEdFpRVU0zUkN4TlFVRk5MRTlCUVU4c1EwRkJReXhqUVVGakxFTkJRVU1zVVVGQlVTeERRVUZETzJkQ1FVTndReXhGUVVGRkxFVkJRVVU3YjBKQlEwWTdkMEpCUTBVc1NVRkJTU3hGUVVGRkxIVkRRVUYxUXl4SlFVRkpMRU5CUVVNc1UwRkJVeXhEUVVONlJDeHRRa0ZCYlVJc1EwRkRjRUlzUjBGQlJ6dHhRa0ZEVER0cFFrRkRSanRuUWtGRFJDeFBRVUZQTEVWQlFVVXNRMEZCUXl4WlFVRlpMRU5CUVVNN1owSkJRM1pDTEZOQlFWTXNSVUZCUlN4SlFVRkpPMmRDUVVObUxFdEJRVXNzUlVGQlJTeG5Ra0ZCWjBJN1owSkJRM1pDTEdWQlFXVXNSVUZCUlN4SlFVRkpPMkZCUTNSQ0xFTkJRVU1zUTBGQlF6dFRRVU5LTzFGQlEwUXNUMEZCVHl4UFFVRlBMRU5CUVVNc1kwRkJZeXhEUVVGRExGRkJRVkVzUTBGQlF6dFpRVU55UXl4RlFVRkZMRVZCUVVVc1EwRkJReXhGUVVGRkxFbEJRVWtzUlVGQlJTeGhRVUZoTEVWQlFVVXNRMEZCUXp0WlFVTTNRaXhQUVVGUExFVkJRVVVzUTBGQlF5eFpRVUZaTEVOQlFVTTdXVUZEZGtJc1UwRkJVeXhGUVVGRkxFbEJRVWs3V1VGRFppeExRVUZMTEVWQlFVVXNaMEpCUVdkQ08xbEJRM1pDTEdWQlFXVXNSVUZCUlN4SlFVRkpPMU5CUTNSQ0xFTkJRVU1zUTBGQlF6dEpRVU5NTEVOQlFVTTdTVUZGVFN4UFFVRlBPMUZCUTFvc1NVRkJTU3hEUVVGRExHTkJRV01zUjBGQlJ5eEZRVUZGTEVOQlFVTTdVVUZEZWtJc1NVRkJTU3hKUVVGSkxFTkJRVU1zYVVKQlFXbENMRVZCUVVVN1dVRkRNVUlzVDBGQlR5eERRVUZETEU5QlFVOHNRMEZCUXl4VFFVRlRMRU5CUVVNc1kwRkJZeXhEUVVGRExFbEJRVWtzUTBGQlF5eHBRa0ZCYVVJc1EwRkJReXhEUVVGRE8xTkJRMnhGTzBsQlEwZ3NRMEZCUXp0RFFVTkdJbjA9IiwiaW1wb3J0IHsgaW5jcmVtZW50ZWRFdmVudE9yZGluYWwgfSBmcm9tIFwiLi4vbGliL2V4dGVuc2lvbi1zZXNzaW9uLWV2ZW50LW9yZGluYWxcIjtcbmltcG9ydCB7IGV4dGVuc2lvblNlc3Npb25VdWlkIH0gZnJvbSBcIi4uL2xpYi9leHRlbnNpb24tc2Vzc2lvbi11dWlkXCI7XG5pbXBvcnQgeyBQZW5kaW5nTmF2aWdhdGlvbiB9IGZyb20gXCIuLi9saWIvcGVuZGluZy1uYXZpZ2F0aW9uXCI7XG5pbXBvcnQgeyBib29sVG9JbnQsIGVzY2FwZVN0cmluZywgZXNjYXBlVXJsIH0gZnJvbSBcIi4uL2xpYi9zdHJpbmctdXRpbHNcIjtcbmltcG9ydCB7IG1ha2VVVUlEIH0gZnJvbSBcIi4uL2xpYi91dWlkXCI7XG5leHBvcnQgY29uc3QgdHJhbnNmb3JtV2ViTmF2aWdhdGlvbkJhc2VFdmVudERldGFpbHNUb09wZW5XUE1TY2hlbWEgPSBhc3luYyAoY3Jhd2xJRCwgZGV0YWlscykgPT4ge1xuICAgIGNvbnN0IHRhYiA9IGRldGFpbHMudGFiSWQgPiAtMVxuICAgICAgICA/IGF3YWl0IGJyb3dzZXIudGFicy5nZXQoZGV0YWlscy50YWJJZClcbiAgICAgICAgOiB7XG4gICAgICAgICAgICB3aW5kb3dJZDogdW5kZWZpbmVkLFxuICAgICAgICAgICAgaW5jb2duaXRvOiB1bmRlZmluZWQsXG4gICAgICAgICAgICBjb29raWVTdG9yZUlkOiB1bmRlZmluZWQsXG4gICAgICAgICAgICBvcGVuZXJUYWJJZDogdW5kZWZpbmVkLFxuICAgICAgICAgICAgd2lkdGg6IHVuZGVmaW5lZCxcbiAgICAgICAgICAgIGhlaWdodDogdW5kZWZpbmVkLFxuICAgICAgICB9O1xuICAgIGNvbnN0IHdpbmRvdyA9IHRhYi53aW5kb3dJZFxuICAgICAgICA/IGF3YWl0IGJyb3dzZXIud2luZG93cy5nZXQodGFiLndpbmRvd0lkKVxuICAgICAgICA6IHsgd2lkdGg6IHVuZGVmaW5lZCwgaGVpZ2h0OiB1bmRlZmluZWQsIHR5cGU6IHVuZGVmaW5lZCB9O1xuICAgIGNvbnN0IG5hdmlnYXRpb24gPSB7XG4gICAgICAgIGJyb3dzZXJfaWQ6IGNyYXdsSUQsXG4gICAgICAgIGluY29nbml0bzogYm9vbFRvSW50KHRhYi5pbmNvZ25pdG8pLFxuICAgICAgICBleHRlbnNpb25fc2Vzc2lvbl91dWlkOiBleHRlbnNpb25TZXNzaW9uVXVpZCxcbiAgICAgICAgcHJvY2Vzc19pZDogZGV0YWlscy5wcm9jZXNzSWQsXG4gICAgICAgIHdpbmRvd19pZDogdGFiLndpbmRvd0lkLFxuICAgICAgICB0YWJfaWQ6IGRldGFpbHMudGFiSWQsXG4gICAgICAgIHRhYl9vcGVuZXJfdGFiX2lkOiB0YWIub3BlbmVyVGFiSWQsXG4gICAgICAgIGZyYW1lX2lkOiBkZXRhaWxzLmZyYW1lSWQsXG4gICAgICAgIHdpbmRvd193aWR0aDogd2luZG93LndpZHRoLFxuICAgICAgICB3aW5kb3dfaGVpZ2h0OiB3aW5kb3cuaGVpZ2h0LFxuICAgICAgICB3aW5kb3dfdHlwZTogd2luZG93LnR5cGUsXG4gICAgICAgIHRhYl93aWR0aDogdGFiLndpZHRoLFxuICAgICAgICB0YWJfaGVpZ2h0OiB0YWIuaGVpZ2h0LFxuICAgICAgICB0YWJfY29va2llX3N0b3JlX2lkOiBlc2NhcGVTdHJpbmcodGFiLmNvb2tpZVN0b3JlSWQpLFxuICAgICAgICB1dWlkOiBtYWtlVVVJRCgpLFxuICAgICAgICB1cmw6IGVzY2FwZVVybChkZXRhaWxzLnVybCksXG4gICAgfTtcbiAgICByZXR1cm4gbmF2aWdhdGlvbjtcbn07XG5leHBvcnQgY2xhc3MgTmF2aWdhdGlvbkluc3RydW1lbnQge1xuICAgIHN0YXRpYyBuYXZpZ2F0aW9uSWQocHJvY2Vzc0lkLCB0YWJJZCwgZnJhbWVJZCkge1xuICAgICAgICByZXR1cm4gYCR7cHJvY2Vzc0lkfS0ke3RhYklkfS0ke2ZyYW1lSWR9YDtcbiAgICB9XG4gICAgZGF0YVJlY2VpdmVyO1xuICAgIG9uQmVmb3JlTmF2aWdhdGVMaXN0ZW5lcjtcbiAgICBvbkNvbW1pdHRlZExpc3RlbmVyO1xuICAgIHBlbmRpbmdOYXZpZ2F0aW9ucyA9IHt9O1xuICAgIGNvbnN0cnVjdG9yKGRhdGFSZWNlaXZlcikge1xuICAgICAgICB0aGlzLmRhdGFSZWNlaXZlciA9IGRhdGFSZWNlaXZlcjtcbiAgICB9XG4gICAgcnVuKGNyYXdsSUQpIHtcbiAgICAgICAgdGhpcy5vbkJlZm9yZU5hdmlnYXRlTGlzdGVuZXIgPSBhc3luYyAoZGV0YWlscykgPT4ge1xuICAgICAgICAgICAgY29uc3QgbmF2aWdhdGlvbklkID0gTmF2aWdhdGlvbkluc3RydW1lbnQubmF2aWdhdGlvbklkKGRldGFpbHMucHJvY2Vzc0lkLCBkZXRhaWxzLnRhYklkLCBkZXRhaWxzLmZyYW1lSWQpO1xuICAgICAgICAgICAgY29uc3QgcGVuZGluZ05hdmlnYXRpb24gPSB0aGlzLmluc3RhbnRpYXRlUGVuZGluZ05hdmlnYXRpb24obmF2aWdhdGlvbklkKTtcbiAgICAgICAgICAgIGNvbnN0IG5hdmlnYXRpb24gPSBhd2FpdCB0cmFuc2Zvcm1XZWJOYXZpZ2F0aW9uQmFzZUV2ZW50RGV0YWlsc1RvT3BlbldQTVNjaGVtYShjcmF3bElELCBkZXRhaWxzKTtcbiAgICAgICAgICAgIG5hdmlnYXRpb24ucGFyZW50X2ZyYW1lX2lkID0gZGV0YWlscy5wYXJlbnRGcmFtZUlkO1xuICAgICAgICAgICAgbmF2aWdhdGlvbi5iZWZvcmVfbmF2aWdhdGVfZXZlbnRfb3JkaW5hbCA9IGluY3JlbWVudGVkRXZlbnRPcmRpbmFsKCk7XG4gICAgICAgICAgICBuYXZpZ2F0aW9uLmJlZm9yZV9uYXZpZ2F0ZV90aW1lX3N0YW1wID0gbmV3IERhdGUoZGV0YWlscy50aW1lU3RhbXApLnRvSVNPU3RyaW5nKCk7XG4gICAgICAgICAgICBwZW5kaW5nTmF2aWdhdGlvbi5yZXNvbHZlT25CZWZvcmVOYXZpZ2F0ZUV2ZW50TmF2aWdhdGlvbihuYXZpZ2F0aW9uKTtcbiAgICAgICAgfTtcbiAgICAgICAgYnJvd3Nlci53ZWJOYXZpZ2F0aW9uLm9uQmVmb3JlTmF2aWdhdGUuYWRkTGlzdGVuZXIodGhpcy5vbkJlZm9yZU5hdmlnYXRlTGlzdGVuZXIpO1xuICAgICAgICB0aGlzLm9uQ29tbWl0dGVkTGlzdGVuZXIgPSBhc3luYyAoZGV0YWlscykgPT4ge1xuICAgICAgICAgICAgY29uc3QgbmF2aWdhdGlvbklkID0gTmF2aWdhdGlvbkluc3RydW1lbnQubmF2aWdhdGlvbklkKGRldGFpbHMucHJvY2Vzc0lkLCBkZXRhaWxzLnRhYklkLCBkZXRhaWxzLmZyYW1lSWQpO1xuICAgICAgICAgICAgY29uc3QgbmF2aWdhdGlvbiA9IGF3YWl0IHRyYW5zZm9ybVdlYk5hdmlnYXRpb25CYXNlRXZlbnREZXRhaWxzVG9PcGVuV1BNU2NoZW1hKGNyYXdsSUQsIGRldGFpbHMpO1xuICAgICAgICAgICAgbmF2aWdhdGlvbi50cmFuc2l0aW9uX3F1YWxpZmllcnMgPSBlc2NhcGVTdHJpbmcoSlNPTi5zdHJpbmdpZnkoZGV0YWlscy50cmFuc2l0aW9uUXVhbGlmaWVycykpO1xuICAgICAgICAgICAgbmF2aWdhdGlvbi50cmFuc2l0aW9uX3R5cGUgPSBlc2NhcGVTdHJpbmcoZGV0YWlscy50cmFuc2l0aW9uVHlwZSk7XG4gICAgICAgICAgICBuYXZpZ2F0aW9uLmNvbW1pdHRlZF9ldmVudF9vcmRpbmFsID0gaW5jcmVtZW50ZWRFdmVudE9yZGluYWwoKTtcbiAgICAgICAgICAgIG5hdmlnYXRpb24uY29tbWl0dGVkX3RpbWVfc3RhbXAgPSBuZXcgRGF0ZShkZXRhaWxzLnRpbWVTdGFtcCkudG9JU09TdHJpbmcoKTtcbiAgICAgICAgICAgIC8vIGluY2x1ZGUgYXR0cmlidXRlcyBmcm9tIHRoZSBjb3JyZXNwb25kaW5nIG9uQmVmb3JlTmF2aWdhdGlvbiBldmVudFxuICAgICAgICAgICAgY29uc3QgcGVuZGluZ05hdmlnYXRpb24gPSB0aGlzLmdldFBlbmRpbmdOYXZpZ2F0aW9uKG5hdmlnYXRpb25JZCk7XG4gICAgICAgICAgICBpZiAocGVuZGluZ05hdmlnYXRpb24pIHtcbiAgICAgICAgICAgICAgICBwZW5kaW5nTmF2aWdhdGlvbi5yZXNvbHZlT25Db21taXR0ZWRFdmVudE5hdmlnYXRpb24obmF2aWdhdGlvbik7XG4gICAgICAgICAgICAgICAgY29uc3QgcmVzb2x2ZWQgPSBhd2FpdCBwZW5kaW5nTmF2aWdhdGlvbi5yZXNvbHZlZFdpdGhpblRpbWVvdXQoMTAwMCk7XG4gICAgICAgICAgICAgICAgaWYgKHJlc29sdmVkKSB7XG4gICAgICAgICAgICAgICAgICAgIGNvbnN0IG9uQmVmb3JlTmF2aWdhdGVFdmVudE5hdmlnYXRpb24gPSBhd2FpdCBwZW5kaW5nTmF2aWdhdGlvbi5vbkJlZm9yZU5hdmlnYXRlRXZlbnROYXZpZ2F0aW9uO1xuICAgICAgICAgICAgICAgICAgICBuYXZpZ2F0aW9uLnBhcmVudF9mcmFtZV9pZCA9XG4gICAgICAgICAgICAgICAgICAgICAgICBvbkJlZm9yZU5hdmlnYXRlRXZlbnROYXZpZ2F0aW9uLnBhcmVudF9mcmFtZV9pZDtcbiAgICAgICAgICAgICAgICAgICAgbmF2aWdhdGlvbi5iZWZvcmVfbmF2aWdhdGVfZXZlbnRfb3JkaW5hbCA9XG4gICAgICAgICAgICAgICAgICAgICAgICBvbkJlZm9yZU5hdmlnYXRlRXZlbnROYXZpZ2F0aW9uLmJlZm9yZV9uYXZpZ2F0ZV9ldmVudF9vcmRpbmFsO1xuICAgICAgICAgICAgICAgICAgICBuYXZpZ2F0aW9uLmJlZm9yZV9uYXZpZ2F0ZV90aW1lX3N0YW1wID1cbiAgICAgICAgICAgICAgICAgICAgICAgIG9uQmVmb3JlTmF2aWdhdGVFdmVudE5hdmlnYXRpb24uYmVmb3JlX25hdmlnYXRlX3RpbWVfc3RhbXA7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIuc2F2ZVJlY29yZChcIm5hdmlnYXRpb25zXCIsIG5hdmlnYXRpb24pO1xuICAgICAgICB9O1xuICAgICAgICBicm93c2VyLndlYk5hdmlnYXRpb24ub25Db21taXR0ZWQuYWRkTGlzdGVuZXIodGhpcy5vbkNvbW1pdHRlZExpc3RlbmVyKTtcbiAgICB9XG4gICAgY2xlYW51cCgpIHtcbiAgICAgICAgaWYgKHRoaXMub25CZWZvcmVOYXZpZ2F0ZUxpc3RlbmVyKSB7XG4gICAgICAgICAgICBicm93c2VyLndlYk5hdmlnYXRpb24ub25CZWZvcmVOYXZpZ2F0ZS5yZW1vdmVMaXN0ZW5lcih0aGlzLm9uQmVmb3JlTmF2aWdhdGVMaXN0ZW5lcik7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKHRoaXMub25Db21taXR0ZWRMaXN0ZW5lcikge1xuICAgICAgICAgICAgYnJvd3Nlci53ZWJOYXZpZ2F0aW9uLm9uQ29tbWl0dGVkLnJlbW92ZUxpc3RlbmVyKHRoaXMub25Db21taXR0ZWRMaXN0ZW5lcik7XG4gICAgICAgIH1cbiAgICB9XG4gICAgaW5zdGFudGlhdGVQZW5kaW5nTmF2aWdhdGlvbihuYXZpZ2F0aW9uSWQpIHtcbiAgICAgICAgdGhpcy5wZW5kaW5nTmF2aWdhdGlvbnNbbmF2aWdhdGlvbklkXSA9IG5ldyBQZW5kaW5nTmF2aWdhdGlvbigpO1xuICAgICAgICByZXR1cm4gdGhpcy5wZW5kaW5nTmF2aWdhdGlvbnNbbmF2aWdhdGlvbklkXTtcbiAgICB9XG4gICAgZ2V0UGVuZGluZ05hdmlnYXRpb24obmF2aWdhdGlvbklkKSB7XG4gICAgICAgIHJldHVybiB0aGlzLnBlbmRpbmdOYXZpZ2F0aW9uc1tuYXZpZ2F0aW9uSWRdO1xuICAgIH1cbn1cbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWJtRjJhV2RoZEdsdmJpMXBibk4wY25WdFpXNTBMbXB6SWl3aWMyOTFjbU5sVW05dmRDSTZJaUlzSW5OdmRYSmpaWE1pT2xzaUxpNHZMaTR2TGk0dmMzSmpMMkpoWTJ0bmNtOTFibVF2Ym1GMmFXZGhkR2x2YmkxcGJuTjBjblZ0Wlc1MExuUnpJbDBzSW01aGJXVnpJanBiWFN3aWJXRndjR2x1WjNNaU9pSkJRVUZCTEU5QlFVOHNSVUZCUlN4MVFrRkJkVUlzUlVGQlJTeE5RVUZOTEhkRFFVRjNReXhEUVVGRE8wRkJRMnBHTEU5QlFVOHNSVUZCUlN4dlFrRkJiMElzUlVGQlJTeE5RVUZOTEN0Q1FVRXJRaXhEUVVGRE8wRkJRM0pGTEU5QlFVOHNSVUZCUlN4cFFrRkJhVUlzUlVGQlJTeE5RVUZOTERKQ1FVRXlRaXhEUVVGRE8wRkJRemxFTEU5QlFVOHNSVUZCUlN4VFFVRlRMRVZCUVVVc1dVRkJXU3hGUVVGRkxGTkJRVk1zUlVGQlJTeE5RVUZOTEhGQ1FVRnhRaXhEUVVGRE8wRkJRM3BGTEU5QlFVOHNSVUZCUlN4UlFVRlJMRVZCUVVVc1RVRkJUU3hoUVVGaExFTkJRVU03UVVGUmRrTXNUVUZCVFN4RFFVRkRMRTFCUVUwc2NVUkJRWEZFTEVkQlFVY3NTMEZCU3l4RlFVTjRSU3hQUVVGUExFVkJRMUFzVDBGQmMwTXNSVUZEYWtJc1JVRkJSVHRKUVVOMlFpeE5RVUZOTEVkQlFVY3NSMEZEVUN4UFFVRlBMRU5CUVVNc1MwRkJTeXhIUVVGSExFTkJRVU1zUTBGQlF6dFJRVU5vUWl4RFFVRkRMRU5CUVVNc1RVRkJUU3hQUVVGUExFTkJRVU1zU1VGQlNTeERRVUZETEVkQlFVY3NRMEZCUXl4UFFVRlBMRU5CUVVNc1MwRkJTeXhEUVVGRE8xRkJRM1pETEVOQlFVTXNRMEZCUXp0WlFVTkZMRkZCUVZFc1JVRkJSU3hUUVVGVE8xbEJRMjVDTEZOQlFWTXNSVUZCUlN4VFFVRlRPMWxCUTNCQ0xHRkJRV0VzUlVGQlJTeFRRVUZUTzFsQlEzaENMRmRCUVZjc1JVRkJSU3hUUVVGVE8xbEJRM1JDTEV0QlFVc3NSVUZCUlN4VFFVRlRPMWxCUTJoQ0xFMUJRVTBzUlVGQlJTeFRRVUZUTzFOQlEyeENMRU5CUVVNN1NVRkRVaXhOUVVGTkxFMUJRVTBzUjBGQlJ5eEhRVUZITEVOQlFVTXNVVUZCVVR0UlFVTjZRaXhEUVVGRExFTkJRVU1zVFVGQlRTeFBRVUZQTEVOQlFVTXNUMEZCVHl4RFFVRkRMRWRCUVVjc1EwRkJReXhIUVVGSExFTkJRVU1zVVVGQlVTeERRVUZETzFGQlEzcERMRU5CUVVNc1EwRkJReXhGUVVGRkxFdEJRVXNzUlVGQlJTeFRRVUZUTEVWQlFVVXNUVUZCVFN4RlFVRkZMRk5CUVZNc1JVRkJSU3hKUVVGSkxFVkJRVVVzVTBGQlV5eEZRVUZGTEVOQlFVTTdTVUZETjBRc1RVRkJUU3hWUVVGVkxFZEJRV1U3VVVGRE4wSXNWVUZCVlN4RlFVRkZMRTlCUVU4N1VVRkRia0lzVTBGQlV5eEZRVUZGTEZOQlFWTXNRMEZCUXl4SFFVRkhMRU5CUVVNc1UwRkJVeXhEUVVGRE8xRkJRMjVETEhOQ1FVRnpRaXhGUVVGRkxHOUNRVUZ2UWp0UlFVTTFReXhWUVVGVkxFVkJRVVVzVDBGQlR5eERRVUZETEZOQlFWTTdVVUZETjBJc1UwRkJVeXhGUVVGRkxFZEJRVWNzUTBGQlF5eFJRVUZSTzFGQlEzWkNMRTFCUVUwc1JVRkJSU3hQUVVGUExFTkJRVU1zUzBGQlN6dFJRVU55UWl4cFFrRkJhVUlzUlVGQlJTeEhRVUZITEVOQlFVTXNWMEZCVnp0UlFVTnNReXhSUVVGUkxFVkJRVVVzVDBGQlR5eERRVUZETEU5QlFVODdVVUZEZWtJc1dVRkJXU3hGUVVGRkxFMUJRVTBzUTBGQlF5eExRVUZMTzFGQlF6RkNMR0ZCUVdFc1JVRkJSU3hOUVVGTkxFTkJRVU1zVFVGQlRUdFJRVU0xUWl4WFFVRlhMRVZCUVVVc1RVRkJUU3hEUVVGRExFbEJRVWs3VVVGRGVFSXNVMEZCVXl4RlFVRkZMRWRCUVVjc1EwRkJReXhMUVVGTE8xRkJRM0JDTEZWQlFWVXNSVUZCUlN4SFFVRkhMRU5CUVVNc1RVRkJUVHRSUVVOMFFpeHRRa0ZCYlVJc1JVRkJSU3haUVVGWkxFTkJRVU1zUjBGQlJ5eERRVUZETEdGQlFXRXNRMEZCUXp0UlFVTndSQ3hKUVVGSkxFVkJRVVVzVVVGQlVTeEZRVUZGTzFGQlEyaENMRWRCUVVjc1JVRkJSU3hUUVVGVExFTkJRVU1zVDBGQlR5eERRVUZETEVkQlFVY3NRMEZCUXp0TFFVTTFRaXhEUVVGRE8wbEJRMFlzVDBGQlR5eFZRVUZWTEVOQlFVTTdRVUZEY0VJc1EwRkJReXhEUVVGRE8wRkJSVVlzVFVGQlRTeFBRVUZQTEc5Q1FVRnZRanRKUVVONFFpeE5RVUZOTEVOQlFVTXNXVUZCV1N4RFFVRkRMRk5CUVZNc1JVRkJSU3hMUVVGTExFVkJRVVVzVDBGQlR6dFJRVU5zUkN4UFFVRlBMRWRCUVVjc1UwRkJVeXhKUVVGSkxFdEJRVXNzU1VGQlNTeFBRVUZQTEVWQlFVVXNRMEZCUXp0SlFVTTFReXhEUVVGRE8wbEJRMmRDTEZsQlFWa3NRMEZCUXp0SlFVTjBRaXgzUWtGQmQwSXNRMEZCUXp0SlFVTjZRaXh0UWtGQmJVSXNRMEZCUXp0SlFVTndRaXhyUWtGQmEwSXNSMEZGZEVJc1JVRkJSU3hEUVVGRE8wbEJSVkFzV1VGQldTeFpRVUZaTzFGQlEzUkNMRWxCUVVrc1EwRkJReXhaUVVGWkxFZEJRVWNzV1VGQldTeERRVUZETzBsQlEyNURMRU5CUVVNN1NVRkZUU3hIUVVGSExFTkJRVU1zVDBGQlR6dFJRVU5vUWl4SlFVRkpMRU5CUVVNc2QwSkJRWGRDTEVkQlFVY3NTMEZCU3l4RlFVTnVReXhQUVVGclJDeEZRVU5zUkN4RlFVRkZPMWxCUTBZc1RVRkJUU3haUVVGWkxFZEJRVWNzYjBKQlFXOUNMRU5CUVVNc1dVRkJXU3hEUVVOd1JDeFBRVUZQTEVOQlFVTXNVMEZCVXl4RlFVTnFRaXhQUVVGUExFTkJRVU1zUzBGQlN5eEZRVU5pTEU5QlFVOHNRMEZCUXl4UFFVRlBMRU5CUTJoQ0xFTkJRVU03V1VGRFJpeE5RVUZOTEdsQ1FVRnBRaXhIUVVGSExFbEJRVWtzUTBGQlF5dzBRa0ZCTkVJc1EwRkJReXhaUVVGWkxFTkJRVU1zUTBGQlF6dFpRVU14UlN4TlFVRk5MRlZCUVZVc1IwRkRaQ3hOUVVGTkxIRkVRVUZ4UkN4RFFVTjZSQ3hQUVVGUExFVkJRMUFzVDBGQlR5eERRVU5TTEVOQlFVTTdXVUZEU2l4VlFVRlZMRU5CUVVNc1pVRkJaU3hIUVVGSExFOUJRVThzUTBGQlF5eGhRVUZoTEVOQlFVTTdXVUZEYmtRc1ZVRkJWU3hEUVVGRExEWkNRVUUyUWl4SFFVRkhMSFZDUVVGMVFpeEZRVUZGTEVOQlFVTTdXVUZEY2tVc1ZVRkJWU3hEUVVGRExEQkNRVUV3UWl4SFFVRkhMRWxCUVVrc1NVRkJTU3hEUVVNNVF5eFBRVUZQTEVOQlFVTXNVMEZCVXl4RFFVTnNRaXhEUVVGRExGZEJRVmNzUlVGQlJTeERRVUZETzFsQlEyaENMR2xDUVVGcFFpeERRVUZETEhORFFVRnpReXhEUVVGRExGVkJRVlVzUTBGQlF5eERRVUZETzFGQlEzWkZMRU5CUVVNc1EwRkJRenRSUVVOR0xFOUJRVThzUTBGQlF5eGhRVUZoTEVOQlFVTXNaMEpCUVdkQ0xFTkJRVU1zVjBGQlZ5eERRVU5vUkN4SlFVRkpMRU5CUVVNc2QwSkJRWGRDTEVOQlF6bENMRU5CUVVNN1VVRkRSaXhKUVVGSkxFTkJRVU1zYlVKQlFXMUNMRWRCUVVjc1MwRkJTeXhGUVVNNVFpeFBRVUUyUXl4RlFVTTNReXhGUVVGRk8xbEJRMFlzVFVGQlRTeFpRVUZaTEVkQlFVY3NiMEpCUVc5Q0xFTkJRVU1zV1VGQldTeERRVU53UkN4UFFVRlBMRU5CUVVNc1UwRkJVeXhGUVVOcVFpeFBRVUZQTEVOQlFVTXNTMEZCU3l4RlFVTmlMRTlCUVU4c1EwRkJReXhQUVVGUExFTkJRMmhDTEVOQlFVTTdXVUZEUml4TlFVRk5MRlZCUVZVc1IwRkRaQ3hOUVVGTkxIRkVRVUZ4UkN4RFFVTjZSQ3hQUVVGUExFVkJRMUFzVDBGQlR5eERRVU5TTEVOQlFVTTdXVUZEU2l4VlFVRlZMRU5CUVVNc2NVSkJRWEZDTEVkQlFVY3NXVUZCV1N4RFFVTTNReXhKUVVGSkxFTkJRVU1zVTBGQlV5eERRVUZETEU5QlFVOHNRMEZCUXl4dlFrRkJiMElzUTBGQlF5eERRVU0zUXl4RFFVRkRPMWxCUTBZc1ZVRkJWU3hEUVVGRExHVkJRV1VzUjBGQlJ5eFpRVUZaTEVOQlFVTXNUMEZCVHl4RFFVRkRMR05CUVdNc1EwRkJReXhEUVVGRE8xbEJRMnhGTEZWQlFWVXNRMEZCUXl4MVFrRkJkVUlzUjBGQlJ5eDFRa0ZCZFVJc1JVRkJSU3hEUVVGRE8xbEJReTlFTEZWQlFWVXNRMEZCUXl4dlFrRkJiMElzUjBGQlJ5eEpRVUZKTEVsQlFVa3NRMEZEZUVNc1QwRkJUeXhEUVVGRExGTkJRVk1zUTBGRGJFSXNRMEZCUXl4WFFVRlhMRVZCUVVVc1EwRkJRenRaUVVWb1FpeHhSVUZCY1VVN1dVRkRja1VzVFVGQlRTeHBRa0ZCYVVJc1IwRkJSeXhKUVVGSkxFTkJRVU1zYjBKQlFXOUNMRU5CUVVNc1dVRkJXU3hEUVVGRExFTkJRVU03V1VGRGJFVXNTVUZCU1N4cFFrRkJhVUlzUlVGQlJUdG5Ra0ZEY2tJc2FVSkJRV2xDTEVOQlFVTXNhVU5CUVdsRExFTkJRVU1zVlVGQlZTeERRVUZETEVOQlFVTTdaMEpCUTJoRkxFMUJRVTBzVVVGQlVTeEhRVUZITEUxQlFVMHNhVUpCUVdsQ0xFTkJRVU1zY1VKQlFYRkNMRU5CUVVNc1NVRkJTU3hEUVVGRExFTkJRVU03WjBKQlEzSkZMRWxCUVVrc1VVRkJVU3hGUVVGRk8yOUNRVU5hTEUxQlFVMHNLMEpCUVN0Q0xFZEJRMjVETEUxQlFVMHNhVUpCUVdsQ0xFTkJRVU1zSzBKQlFTdENMRU5CUVVNN2IwSkJRekZFTEZWQlFWVXNRMEZCUXl4bFFVRmxPM2RDUVVONFFpd3JRa0ZCSzBJc1EwRkJReXhsUVVGbExFTkJRVU03YjBKQlEyeEVMRlZCUVZVc1EwRkJReXcyUWtGQk5rSTdkMEpCUTNSRExDdENRVUVyUWl4RFFVRkRMRFpDUVVFMlFpeERRVUZETzI5Q1FVTm9SU3hWUVVGVkxFTkJRVU1zTUVKQlFUQkNPM2RDUVVOdVF5d3JRa0ZCSzBJc1EwRkJReXd3UWtGQk1FSXNRMEZCUXp0cFFrRkRPVVE3WVVGRFJqdFpRVVZFTEVsQlFVa3NRMEZCUXl4WlFVRlpMRU5CUVVNc1ZVRkJWU3hEUVVGRExHRkJRV0VzUlVGQlJTeFZRVUZWTEVOQlFVTXNRMEZCUXp0UlFVTXhSQ3hEUVVGRExFTkJRVU03VVVGRFJpeFBRVUZQTEVOQlFVTXNZVUZCWVN4RFFVRkRMRmRCUVZjc1EwRkJReXhYUVVGWExFTkJRVU1zU1VGQlNTeERRVUZETEcxQ1FVRnRRaXhEUVVGRExFTkJRVU03U1VGRE1VVXNRMEZCUXp0SlFVVk5MRTlCUVU4N1VVRkRXaXhKUVVGSkxFbEJRVWtzUTBGQlF5eDNRa0ZCZDBJc1JVRkJSVHRaUVVOcVF5eFBRVUZQTEVOQlFVTXNZVUZCWVN4RFFVRkRMR2RDUVVGblFpeERRVUZETEdOQlFXTXNRMEZEYmtRc1NVRkJTU3hEUVVGRExIZENRVUYzUWl4RFFVTTVRaXhEUVVGRE8xTkJRMGc3VVVGRFJDeEpRVUZKTEVsQlFVa3NRMEZCUXl4dFFrRkJiVUlzUlVGQlJUdFpRVU0xUWl4UFFVRlBMRU5CUVVNc1lVRkJZU3hEUVVGRExGZEJRVmNzUTBGQlF5eGpRVUZqTEVOQlF6bERMRWxCUVVrc1EwRkJReXh0UWtGQmJVSXNRMEZEZWtJc1EwRkJRenRUUVVOSU8wbEJRMGdzUTBGQlF6dEpRVVZQTERSQ1FVRTBRaXhEUVVOc1F5eFpRVUZ2UWp0UlFVVndRaXhKUVVGSkxFTkJRVU1zYTBKQlFXdENMRU5CUVVNc1dVRkJXU3hEUVVGRExFZEJRVWNzU1VGQlNTeHBRa0ZCYVVJc1JVRkJSU3hEUVVGRE8xRkJRMmhGTEU5QlFVOHNTVUZCU1N4RFFVRkRMR3RDUVVGclFpeERRVUZETEZsQlFWa3NRMEZCUXl4RFFVRkRPMGxCUXk5RExFTkJRVU03U1VGRlR5eHZRa0ZCYjBJc1EwRkJReXhaUVVGdlFqdFJRVU12UXl4UFFVRlBMRWxCUVVrc1EwRkJReXhyUWtGQmEwSXNRMEZCUXl4WlFVRlpMRU5CUVVNc1EwRkJRenRKUVVNdlF5eERRVUZETzBOQlEwWWlmUT09IiwiaW1wb3J0IHsgZ2V0SW5zdHJ1bWVudEpTIH0gZnJvbSBcIi4uL2xpYi9qcy1pbnN0cnVtZW50c1wiO1xuaW1wb3J0IHsgcGFnZVNjcmlwdCB9IGZyb20gXCIuL2phdmFzY3JpcHQtaW5zdHJ1bWVudC1wYWdlLXNjb3BlXCI7XG5mdW5jdGlvbiBnZXRQYWdlU2NyaXB0QXNTdHJpbmcoanNJbnN0cnVtZW50YXRpb25TZXR0aW5ncykge1xuICAgIC8vIFRoZSBKUyBJbnN0cnVtZW50IFJlcXVlc3RzIGFyZSBzZXR1cCBhbmQgdmFsaWRhdGVkIHB5dGhvbiBzaWRlXG4gICAgLy8gaW5jbHVkaW5nIHNldHRpbmcgZGVmYXVsdHMgZm9yIGxvZ1NldHRpbmdzLiBTZWUgSlNJbnN0cnVtZW50YXRpb24ucHlcbiAgICBjb25zdCBwYWdlU2NyaXB0U3RyaW5nID0gYFxuLy8gU3RhcnQgb2YganMtaW5zdHJ1bWVudHMuXG4ke2dldEluc3RydW1lbnRKU31cbi8vIEVuZCBvZiBqcy1pbnN0cnVtZW50cy5cblxuLy8gU3RhcnQgb2YgY3VzdG9tIGluc3RydW1lbnRSZXF1ZXN0cy5cbmNvbnN0IGpzSW5zdHJ1bWVudGF0aW9uU2V0dGluZ3MgPSAke0pTT04uc3RyaW5naWZ5KGpzSW5zdHJ1bWVudGF0aW9uU2V0dGluZ3MpfTtcbi8vIEVuZCBvZiBjdXN0b20gaW5zdHJ1bWVudFJlcXVlc3RzLlxuXG4vLyBTdGFydCBvZiBhbm9ueW1vdXMgZnVuY3Rpb24gZnJvbSBqYXZhc2NyaXB0LWluc3RydW1lbnQtcGFnZS1zY29wZS50c1xuKCR7cGFnZVNjcmlwdH0oZ2V0SW5zdHJ1bWVudEpTLCBqc0luc3RydW1lbnRhdGlvblNldHRpbmdzKSk7XG4vLyBFbmQuXG4gIGA7XG4gICAgcmV0dXJuIHBhZ2VTY3JpcHRTdHJpbmc7XG59XG47XG5mdW5jdGlvbiBpbnNlcnRTY3JpcHQocGFnZVNjcmlwdFN0cmluZywgZXZlbnRJZCwgdGVzdGluZyA9IGZhbHNlKSB7XG4gICAgY29uc3QgcGFyZW50ID0gZG9jdW1lbnQuZG9jdW1lbnRFbGVtZW50O1xuICAgIGNvbnN0IHNjcmlwdCA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJzY3JpcHRcIik7XG4gICAgc2NyaXB0LnRleHQgPSBwYWdlU2NyaXB0U3RyaW5nO1xuICAgIHNjcmlwdC5hc3luYyA9IGZhbHNlO1xuICAgIHNjcmlwdC5zZXRBdHRyaWJ1dGUoXCJkYXRhLWV2ZW50LWlkXCIsIGV2ZW50SWQpO1xuICAgIHNjcmlwdC5zZXRBdHRyaWJ1dGUoXCJkYXRhLXRlc3RpbmdcIiwgYCR7dGVzdGluZ31gKTtcbiAgICBwYXJlbnQuaW5zZXJ0QmVmb3JlKHNjcmlwdCwgcGFyZW50LmZpcnN0Q2hpbGQpO1xuICAgIHBhcmVudC5yZW1vdmVDaGlsZChzY3JpcHQpO1xufVxuO1xuZnVuY3Rpb24gZW1pdE1zZyh0eXBlLCBtc2cpIHtcbiAgICBtc2cudGltZVN0YW1wID0gbmV3IERhdGUoKS50b0lTT1N0cmluZygpO1xuICAgIGJyb3dzZXIucnVudGltZS5zZW5kTWVzc2FnZSh7XG4gICAgICAgIG5hbWVzcGFjZTogXCJqYXZhc2NyaXB0LWluc3RydW1lbnRhdGlvblwiLFxuICAgICAgICB0eXBlLFxuICAgICAgICBkYXRhOiBtc2csXG4gICAgfSk7XG59XG47XG5jb25zdCBldmVudElkID0gTWF0aC5yYW5kb20oKS50b1N0cmluZygpO1xuLy8gbGlzdGVuIGZvciBtZXNzYWdlcyBmcm9tIHRoZSBzY3JpcHQgd2UgYXJlIGFib3V0IHRvIGluc2VydFxuZG9jdW1lbnQuYWRkRXZlbnRMaXN0ZW5lcihldmVudElkLCAoZSkgPT4ge1xuICAgIC8vIHBhc3MgdGhlc2Ugb24gdG8gdGhlIGJhY2tncm91bmQgcGFnZVxuICAgIGNvbnN0IG1zZ3MgPSBlLmRldGFpbDtcbiAgICBpZiAoQXJyYXkuaXNBcnJheShtc2dzKSkge1xuICAgICAgICBtc2dzLmZvckVhY2goKG1zZykgPT4ge1xuICAgICAgICAgICAgZW1pdE1zZyhtc2cudHlwZSwgbXNnLmNvbnRlbnQpO1xuICAgICAgICB9KTtcbiAgICB9XG4gICAgZWxzZSB7XG4gICAgICAgIGVtaXRNc2cobXNncy50eXBlLCBtc2dzLmNvbnRlbnQpO1xuICAgIH1cbn0pO1xuZXhwb3J0IGNvbnN0IGluamVjdEphdmFzY3JpcHRJbnN0cnVtZW50UGFnZVNjcmlwdCA9IChjb250ZW50U2NyaXB0Q29uZmlnKSA9PiB7XG4gICAgaW5zZXJ0U2NyaXB0KGdldFBhZ2VTY3JpcHRBc1N0cmluZyhjb250ZW50U2NyaXB0Q29uZmlnLmpzSW5zdHJ1bWVudGF0aW9uU2V0dGluZ3MpLCBldmVudElkLCBjb250ZW50U2NyaXB0Q29uZmlnLnRlc3RpbmcpO1xufTtcbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWFtRjJZWE5qY21sd2RDMXBibk4wY25WdFpXNTBMV052Ym5SbGJuUXRjMk52Y0dVdWFuTWlMQ0p6YjNWeVkyVlNiMjkwSWpvaUlpd2ljMjkxY21ObGN5STZXeUl1TGk4dUxpOHVMaTl6Y21NdlkyOXVkR1Z1ZEM5cVlYWmhjMk55YVhCMExXbHVjM1J5ZFcxbGJuUXRZMjl1ZEdWdWRDMXpZMjl3WlM1MGN5SmRMQ0p1WVcxbGN5STZXMTBzSW0xaGNIQnBibWR6SWpvaVFVRkJRU3hQUVVGUExFVkJRVVVzWlVGQlpTeEZRVUYxUWl4TlFVRk5MSFZDUVVGMVFpeERRVUZETzBGQlF6ZEZMRTlCUVU4c1JVRkJSU3hWUVVGVkxFVkJRVVVzVFVGQlRTeHZRMEZCYjBNc1EwRkJRenRCUVVkb1JTeFRRVUZUTEhGQ1FVRnhRaXhEUVVNMVFpeDVRa0ZCWjBRN1NVRkZhRVFzYVVWQlFXbEZPMGxCUTJwRkxIVkZRVUYxUlR0SlFVTjJSU3hOUVVGTkxHZENRVUZuUWl4SFFVRkhPenRGUVVWNlFpeGxRVUZsT3pzN08yOURRVWx0UWl4SlFVRkpMRU5CUVVNc1UwRkJVeXhEUVVGRExIbENRVUY1UWl4RFFVRkRPenM3TzBkQlNURkZMRlZCUVZVN08wZEJSVllzUTBGQlF6dEpRVU5HTEU5QlFVOHNaMEpCUVdkQ0xFTkJRVU03UVVGRE1VSXNRMEZCUXp0QlFVRkJMRU5CUVVNN1FVRkZSaXhUUVVGVExGbEJRVmtzUTBGRGJrSXNaMEpCUVhkQ0xFVkJRM2hDTEU5QlFXVXNSVUZEWml4VlFVRnRRaXhMUVVGTE8wbEJSWGhDTEUxQlFVMHNUVUZCVFN4SFFVRkhMRkZCUVZFc1EwRkJReXhsUVVGbExFTkJRVU03U1VGRGVFTXNUVUZCVFN4TlFVRk5MRWRCUVVjc1VVRkJVU3hEUVVGRExHRkJRV0VzUTBGQlF5eFJRVUZSTEVOQlFVTXNRMEZCUXp0SlFVTm9SQ3hOUVVGTkxFTkJRVU1zU1VGQlNTeEhRVUZITEdkQ1FVRm5RaXhEUVVGRE8wbEJReTlDTEUxQlFVMHNRMEZCUXl4TFFVRkxMRWRCUVVjc1MwRkJTeXhEUVVGRE8wbEJRM0pDTEUxQlFVMHNRMEZCUXl4WlFVRlpMRU5CUVVNc1pVRkJaU3hGUVVGRkxFOUJRVThzUTBGQlF5eERRVUZETzBsQlF6bERMRTFCUVUwc1EwRkJReXhaUVVGWkxFTkJRVU1zWTBGQll5eEZRVUZGTEVkQlFVY3NUMEZCVHl4RlFVRkZMRU5CUVVNc1EwRkJRenRKUVVOc1JDeE5RVUZOTEVOQlFVTXNXVUZCV1N4RFFVRkRMRTFCUVUwc1JVRkJSU3hOUVVGTkxFTkJRVU1zVlVGQlZTeERRVUZETEVOQlFVTTdTVUZETDBNc1RVRkJUU3hEUVVGRExGZEJRVmNzUTBGQlF5eE5RVUZOTEVOQlFVTXNRMEZCUXp0QlFVTTNRaXhEUVVGRE8wRkJRVUVzUTBGQlF6dEJRVVZHTEZOQlFWTXNUMEZCVHl4RFFVRkZMRWxCUVVrc1JVRkJSU3hIUVVGSE8wbEJRM3BDTEVkQlFVY3NRMEZCUXl4VFFVRlRMRWRCUVVjc1NVRkJTU3hKUVVGSkxFVkJRVVVzUTBGQlF5eFhRVUZYTEVWQlFVVXNRMEZCUXp0SlFVTjZReXhQUVVGUExFTkJRVU1zVDBGQlR5eERRVUZETEZkQlFWY3NRMEZCUXp0UlFVTXhRaXhUUVVGVExFVkJRVVVzTkVKQlFUUkNPMUZCUTNaRExFbEJRVWs3VVVGRFNpeEpRVUZKTEVWQlFVVXNSMEZCUnp0TFFVTldMRU5CUVVNc1EwRkJRenRCUVVOTUxFTkJRVU03UVVGQlFTeERRVUZETzBGQlJVWXNUVUZCVFN4UFFVRlBMRWRCUVVjc1NVRkJTU3hEUVVGRExFMUJRVTBzUlVGQlJTeERRVUZETEZGQlFWRXNSVUZCUlN4RFFVRkRPMEZCUlhwRExEWkVRVUUyUkR0QlFVTTNSQ3hSUVVGUkxFTkJRVU1zWjBKQlFXZENMRU5CUVVNc1QwRkJUeXhGUVVGRkxFTkJRVU1zUTBGQll5eEZRVUZGTEVWQlFVVTdTVUZEY0VRc2RVTkJRWFZETzBsQlEzWkRMRTFCUVUwc1NVRkJTU3hIUVVGSExFTkJRVU1zUTBGQlF5eE5RVUZOTEVOQlFVTTdTVUZEZEVJc1NVRkJTU3hMUVVGTExFTkJRVU1zVDBGQlR5eERRVUZETEVsQlFVa3NRMEZCUXl4RlFVRkZPMUZCUTNaQ0xFbEJRVWtzUTBGQlF5eFBRVUZQTEVOQlFVTXNRMEZCUXl4SFFVRkhMRVZCUVVVc1JVRkJSVHRaUVVOdVFpeFBRVUZQTEVOQlFVTXNSMEZCUnl4RFFVRkRMRWxCUVVrc1JVRkJSU3hIUVVGSExFTkJRVU1zVDBGQlR5eERRVUZETEVOQlFVTTdVVUZEYWtNc1EwRkJReXhEUVVGRExFTkJRVU03UzBGRFNqdFRRVUZOTzFGQlEwd3NUMEZCVHl4RFFVRkRMRWxCUVVrc1EwRkJReXhKUVVGSkxFVkJRVVVzU1VGQlNTeERRVUZETEU5QlFVOHNRMEZCUXl4RFFVRkRPMHRCUTJ4RE8wRkJRMGdzUTBGQlF5eERRVUZETEVOQlFVTTdRVUZGU0N4TlFVRk5MRU5CUVVNc1RVRkJUU3h2UTBGQmIwTXNSMEZCUnl4RFFVRkRMRzFDUVVFclF5eEZRVUZGTEVWQlFVVTdTVUZEZEVjc1dVRkJXU3hEUVVOV0xIRkNRVUZ4UWl4RFFVRkRMRzFDUVVGdFFpeERRVUZETEhsQ1FVRjVRaXhEUVVGRExFVkJRM0JGTEU5QlFVOHNSVUZEVUN4dFFrRkJiVUlzUTBGQlF5eFBRVUZQTEVOQlF6VkNMRU5CUVVNN1FVRkRTaXhEUVVGRExFTkJRVUVpZlE9PSIsIi8qIGVzbGludC1kaXNhYmxlIG5vLWNvbnNvbGUgKi9cbi8vIENvZGUgYmVsb3cgaXMgbm90IGEgY29udGVudCBzY3JpcHQ6IG5vIEZpcmVmb3ggQVBJcyBzaG91bGQgYmUgdXNlZFxuLy8gQWxzbywgbm8gd2VicGFjay9lczYgaW1wb3J0cyBtYXkgYmUgdXNlZCBpbiB0aGlzIGZpbGUgc2luY2UgdGhlIHNjcmlwdFxuLy8gaXMgZXhwb3J0ZWQgYXMgYSBwYWdlIHNjcmlwdCBhcyBhIHN0cmluZ1xuZXhwb3J0IGZ1bmN0aW9uIHBhZ2VTY3JpcHQoZ2V0SW5zdHJ1bWVudEpTLCBqc0luc3RydW1lbnRhdGlvblNldHRpbmdzKSB7XG4gICAgLy8gbWVzc2FnZXMgdGhlIGluamVjdGVkIHNjcmlwdFxuICAgIGNvbnN0IHNlbmRNZXNzYWdlc1RvTG9nZ2VyID0gKGV2ZW50SWQsIG1lc3NhZ2VzKSA9PiB7XG4gICAgICAgIGRvY3VtZW50LmRpc3BhdGNoRXZlbnQobmV3IEN1c3RvbUV2ZW50KGV2ZW50SWQsIHtcbiAgICAgICAgICAgIGRldGFpbDogbWVzc2FnZXMsXG4gICAgICAgIH0pKTtcbiAgICB9O1xuICAgIGNvbnN0IGV2ZW50SWQgPSBkb2N1bWVudC5jdXJyZW50U2NyaXB0LmdldEF0dHJpYnV0ZShcImRhdGEtZXZlbnQtaWRcIik7XG4gICAgY29uc3QgdGVzdGluZyA9IGRvY3VtZW50LmN1cnJlbnRTY3JpcHQuZ2V0QXR0cmlidXRlKFwiZGF0YS10ZXN0aW5nXCIpO1xuICAgIGNvbnN0IGluc3RydW1lbnRKUyA9IGdldEluc3RydW1lbnRKUyhldmVudElkLCBzZW5kTWVzc2FnZXNUb0xvZ2dlcik7XG4gICAgbGV0IHQwO1xuICAgIGlmICh0ZXN0aW5nID09PSBcInRydWVcIikge1xuICAgICAgICBjb25zb2xlLmxvZyhcIk9wZW5XUE06IEN1cnJlbnRseSB0ZXN0aW5nXCIpO1xuICAgICAgICB0MCA9IHBlcmZvcm1hbmNlLm5vdygpO1xuICAgICAgICBjb25zb2xlLmxvZyhcIkJlZ2luIGxvYWRpbmcgSlMgaW5zdHJ1bWVudGF0aW9uLlwiKTtcbiAgICB9XG4gICAgaW5zdHJ1bWVudEpTKGpzSW5zdHJ1bWVudGF0aW9uU2V0dGluZ3MpO1xuICAgIGlmICh0ZXN0aW5nID09PSBcInRydWVcIikge1xuICAgICAgICBjb25zdCB0MSA9IHBlcmZvcm1hbmNlLm5vdygpO1xuICAgICAgICBjb25zb2xlLmxvZyhgQ2FsbCB0byBpbnN0cnVtZW50SlMgdG9vayAke3QxIC0gdDB9IG1pbGxpc2Vjb25kcy5gKTtcbiAgICAgICAgd2luZG93Lmluc3RydW1lbnRKUyA9IGluc3RydW1lbnRKUztcbiAgICAgICAgY29uc29sZS5sb2coXCJPcGVuV1BNOiBDb250ZW50LXNpZGUgamF2YXNjcmlwdCBpbnN0cnVtZW50YXRpb24gc3RhcnRlZCB3aXRoIHNwZWM6XCIsIGpzSW5zdHJ1bWVudGF0aW9uU2V0dGluZ3MsIG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSwgXCIoaWYgc3BlYyBpcyAnPHVuYXZhaWxhYmxlPicgY2hlY2sgd2ViIGNvbnNvbGUuKVwiKTtcbiAgICB9XG59XG47XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2lhbUYyWVhOamNtbHdkQzFwYm5OMGNuVnRaVzUwTFhCaFoyVXRjMk52Y0dVdWFuTWlMQ0p6YjNWeVkyVlNiMjkwSWpvaUlpd2ljMjkxY21ObGN5STZXeUl1TGk4dUxpOHVMaTl6Y21NdlkyOXVkR1Z1ZEM5cVlYWmhjMk55YVhCMExXbHVjM1J5ZFcxbGJuUXRjR0ZuWlMxelkyOXdaUzUwY3lKZExDSnVZVzFsY3lJNlcxMHNJbTFoY0hCcGJtZHpJam9pUVVGQlFTd3JRa0ZCSzBJN1FVRkRMMElzY1VWQlFYRkZPMEZCUTNKRkxIbEZRVUY1UlR0QlFVTjZSU3d5UTBGQk1rTTdRVUZGTTBNc1RVRkJUU3hWUVVGVkxGVkJRVlVzUTBGQlJTeGxRVUZsTEVWQlFVVXNlVUpCUVhsQ08wbEJRM0JGTEN0Q1FVRXJRanRKUVVNdlFpeE5RVUZOTEc5Q1FVRnZRaXhIUVVGSExFTkJRVU1zVDBGQlR5eEZRVUZGTEZGQlFWRXNSVUZCUlN4RlFVRkZPMUZCUTJwRUxGRkJRVkVzUTBGQlF5eGhRVUZoTEVOQlEzQkNMRWxCUVVrc1YwRkJWeXhEUVVGRExFOUJRVThzUlVGQlJUdFpRVU4yUWl4TlFVRk5MRVZCUVVVc1VVRkJVVHRUUVVOcVFpeERRVUZETEVOQlEwZ3NRMEZCUXp0SlFVTktMRU5CUVVNc1EwRkJRenRKUVVWR0xFMUJRVTBzVDBGQlR5eEhRVUZITEZGQlFWRXNRMEZCUXl4aFFVRmhMRU5CUVVNc1dVRkJXU3hEUVVGRExHVkJRV1VzUTBGQlF5eERRVUZETzBsQlEzSkZMRTFCUVUwc1QwRkJUeXhIUVVGSExGRkJRVkVzUTBGQlF5eGhRVUZoTEVOQlFVTXNXVUZCV1N4RFFVRkRMR05CUVdNc1EwRkJReXhEUVVGRE8wbEJRM0JGTEUxQlFVMHNXVUZCV1N4SFFVRkhMR1ZCUVdVc1EwRkJReXhQUVVGUExFVkJRVVVzYjBKQlFXOUNMRU5CUVVNc1EwRkJRenRKUVVOd1JTeEpRVUZKTEVWQlFWVXNRMEZCUXp0SlFVTm1MRWxCUVVrc1QwRkJUeXhMUVVGTExFMUJRVTBzUlVGQlJUdFJRVU4wUWl4UFFVRlBMRU5CUVVNc1IwRkJSeXhEUVVGRExEUkNRVUUwUWl4RFFVRkRMRU5CUVVNN1VVRkRNVU1zUlVGQlJTeEhRVUZITEZkQlFWY3NRMEZCUXl4SFFVRkhMRVZCUVVVc1EwRkJRenRSUVVOMlFpeFBRVUZQTEVOQlFVTXNSMEZCUnl4RFFVRkRMRzFEUVVGdFF5eERRVUZETEVOQlFVTTdTMEZEYkVRN1NVRkRSQ3haUVVGWkxFTkJRVU1zZVVKQlFYbENMRU5CUVVNc1EwRkJRenRKUVVONFF5eEpRVUZKTEU5QlFVOHNTMEZCU3l4TlFVRk5MRVZCUVVVN1VVRkRkRUlzVFVGQlRTeEZRVUZGTEVkQlFVY3NWMEZCVnl4RFFVRkRMRWRCUVVjc1JVRkJSU3hEUVVGRE8xRkJRemRDTEU5QlFVOHNRMEZCUXl4SFFVRkhMRU5CUVVNc05rSkJRVFpDTEVWQlFVVXNSMEZCUnl4RlFVRkZMR2RDUVVGblFpeERRVUZETEVOQlFVTTdVVUZEYWtVc1RVRkJZeXhEUVVGRExGbEJRVmtzUjBGQlJ5eFpRVUZaTEVOQlFVTTdVVUZETlVNc1QwRkJUeXhEUVVGRExFZEJRVWNzUTBGRFZDeHhSVUZCY1VVc1JVRkRja1VzZVVKQlFYbENMRVZCUTNwQ0xFbEJRVWtzU1VGQlNTeEZRVUZGTEVOQlFVTXNWMEZCVnl4RlFVRkZMRVZCUTNoQ0xHbEVRVUZwUkN4RFFVTnNSQ3hEUVVGRE8wdEJRMGc3UVVGRFNDeERRVUZETzBGQlFVRXNRMEZCUXlKOSIsImV4cG9ydCAqIGZyb20gXCIuL2JhY2tncm91bmQvY29va2llLWluc3RydW1lbnRcIjtcbmV4cG9ydCAqIGZyb20gXCIuL2JhY2tncm91bmQvZG5zLWluc3RydW1lbnRcIjtcbmV4cG9ydCAqIGZyb20gXCIuL2JhY2tncm91bmQvaHR0cC1pbnN0cnVtZW50XCI7XG5leHBvcnQgKiBmcm9tIFwiLi9iYWNrZ3JvdW5kL2phdmFzY3JpcHQtaW5zdHJ1bWVudFwiO1xuZXhwb3J0ICogZnJvbSBcIi4vYmFja2dyb3VuZC9uYXZpZ2F0aW9uLWluc3RydW1lbnRcIjtcbmV4cG9ydCAqIGZyb20gXCIuL2NvbnRlbnQvamF2YXNjcmlwdC1pbnN0cnVtZW50LWNvbnRlbnQtc2NvcGVcIjtcbmV4cG9ydCAqIGZyb20gXCIuL2xpYi9odHRwLXBvc3QtcGFyc2VyXCI7XG5leHBvcnQgKiBmcm9tIFwiLi9saWIvc3RyaW5nLXV0aWxzXCI7XG5leHBvcnQgKiBmcm9tIFwiLi9zY2hlbWFcIjtcbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWFXNWtaWGd1YW5NaUxDSnpiM1Z5WTJWU2IyOTBJam9pSWl3aWMyOTFjbU5sY3lJNld5SXVMaTh1TGk5emNtTXZhVzVrWlhndWRITWlYU3dpYm1GdFpYTWlPbHRkTENKdFlYQndhVzVuY3lJNklrRkJRVUVzWTBGQll5eG5RMEZCWjBNc1EwRkJRenRCUVVNdlF5eGpRVUZqTERaQ1FVRTJRaXhEUVVGRE8wRkJRelZETEdOQlFXTXNPRUpCUVRoQ0xFTkJRVU03UVVGRE4wTXNZMEZCWXl4dlEwRkJiME1zUTBGQlF6dEJRVU51UkN4alFVRmpMRzlEUVVGdlF5eERRVUZETzBGQlEyNUVMR05CUVdNc0swTkJRU3RETEVOQlFVTTdRVUZET1VRc1kwRkJZeXgzUWtGQmQwSXNRMEZCUXp0QlFVTjJReXhqUVVGakxHOUNRVUZ2UWl4RFFVRkRPMEZCUTI1RExHTkJRV01zVlVGQlZTeERRVUZESW4wPSIsIi8qKlxuICogVGhpcyBlbmFibGVzIHVzIHRvIGtlZXAgaW5mb3JtYXRpb24gYWJvdXQgdGhlIG9yaWdpbmFsIG9yZGVyXG4gKiBpbiB3aGljaCBldmVudHMgYXJyaXZlZCB0byBvdXIgZXZlbnQgbGlzdGVuZXJzLlxuICovXG5sZXQgZXZlbnRPcmRpbmFsID0gMDtcbmV4cG9ydCBjb25zdCBpbmNyZW1lbnRlZEV2ZW50T3JkaW5hbCA9ICgpID0+IHtcbiAgICByZXR1cm4gZXZlbnRPcmRpbmFsKys7XG59O1xuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pWlhoMFpXNXphVzl1TFhObGMzTnBiMjR0WlhabGJuUXRiM0prYVc1aGJDNXFjeUlzSW5OdmRYSmpaVkp2YjNRaU9pSWlMQ0p6YjNWeVkyVnpJanBiSWk0dUx5NHVMeTR1TDNOeVl5OXNhV0l2WlhoMFpXNXphVzl1TFhObGMzTnBiMjR0WlhabGJuUXRiM0prYVc1aGJDNTBjeUpkTENKdVlXMWxjeUk2VzEwc0ltMWhjSEJwYm1keklqb2lRVUZCUVRzN08wZEJSMGM3UVVGRFNDeEpRVUZKTEZsQlFWa3NSMEZCUnl4RFFVRkRMRU5CUVVNN1FVRkZja0lzVFVGQlRTeERRVUZETEUxQlFVMHNkVUpCUVhWQ0xFZEJRVWNzUjBGQlJ5eEZRVUZGTzBsQlF6RkRMRTlCUVU4c1dVRkJXU3hGUVVGRkxFTkJRVU03UVVGRGVFSXNRMEZCUXl4RFFVRkRJbjA9IiwiaW1wb3J0IHsgbWFrZVVVSUQgfSBmcm9tIFwiLi91dWlkXCI7XG4vKipcbiAqIFRoaXMgZW5hYmxlcyB1cyB0byBhY2Nlc3MgYSB1bmlxdWUgcmVmZXJlbmNlIHRvIHRoaXMgYnJvd3NlclxuICogc2Vzc2lvbiAtIHJlZ2VuZXJhdGVkIGFueSB0aW1lIHRoZSBiYWNrZ3JvdW5kIHByb2Nlc3MgZ2V0c1xuICogcmVzdGFydGVkICh3aGljaCBzaG91bGQgb25seSBiZSBvbiBicm93c2VyIHJlc3RhcnRzKVxuICovXG5leHBvcnQgY29uc3QgZXh0ZW5zaW9uU2Vzc2lvblV1aWQgPSBtYWtlVVVJRCgpO1xuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pWlhoMFpXNXphVzl1TFhObGMzTnBiMjR0ZFhWcFpDNXFjeUlzSW5OdmRYSmpaVkp2YjNRaU9pSWlMQ0p6YjNWeVkyVnpJanBiSWk0dUx5NHVMeTR1TDNOeVl5OXNhV0l2WlhoMFpXNXphVzl1TFhObGMzTnBiMjR0ZFhWcFpDNTBjeUpkTENKdVlXMWxjeUk2VzEwc0ltMWhjSEJwYm1keklqb2lRVUZCUVN4UFFVRlBMRVZCUVVVc1VVRkJVU3hGUVVGRkxFMUJRVTBzVVVGQlVTeERRVUZETzBGQlJXeERPenM3TzBkQlNVYzdRVUZEU0N4TlFVRk5MRU5CUVVNc1RVRkJUU3h2UWtGQmIwSXNSMEZCUnl4UlFVRlJMRVZCUVVVc1EwRkJReUo5IiwiaW1wb3J0IHsgZXNjYXBlU3RyaW5nLCBVaW50OFRvQmFzZTY0IH0gZnJvbSBcIi4vc3RyaW5nLXV0aWxzXCI7XG5leHBvcnQgY2xhc3MgSHR0cFBvc3RQYXJzZXIge1xuICAgIG9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscztcbiAgICBkYXRhUmVjZWl2ZXI7XG4gICAgY29uc3RydWN0b3Iob25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzLCBkYXRhUmVjZWl2ZXIpIHtcbiAgICAgICAgdGhpcy5vbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHMgPSBvbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHM7XG4gICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyID0gZGF0YVJlY2VpdmVyO1xuICAgIH1cbiAgICBwYXJzZVBvc3RSZXF1ZXN0KCkge1xuICAgICAgICBjb25zdCByZXF1ZXN0Qm9keSA9IHRoaXMub25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzLnJlcXVlc3RCb2R5O1xuICAgICAgICBpZiAocmVxdWVzdEJvZHkuZXJyb3IpIHtcbiAgICAgICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLmxvZ0Vycm9yKFwiRXhjZXB0aW9uOiBVcHN0cmVhbSBmYWlsZWQgdG8gcGFyc2UgUE9TVDogXCIgKyByZXF1ZXN0Qm9keS5lcnJvcik7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKHJlcXVlc3RCb2R5LmZvcm1EYXRhKSB7XG4gICAgICAgICAgICByZXR1cm4ge1xuICAgICAgICAgICAgICAgIHBvc3RfYm9keTogZXNjYXBlU3RyaW5nKEpTT04uc3RyaW5naWZ5KHJlcXVlc3RCb2R5LmZvcm1EYXRhKSksXG4gICAgICAgICAgICB9O1xuICAgICAgICB9XG4gICAgICAgIGlmIChyZXF1ZXN0Qm9keS5yYXcpIHtcbiAgICAgICAgICAgIHJldHVybiB7XG4gICAgICAgICAgICAgICAgcG9zdF9ib2R5X3JhdzogSlNPTi5zdHJpbmdpZnkocmVxdWVzdEJvZHkucmF3Lm1hcCgoeCkgPT4gW1xuICAgICAgICAgICAgICAgICAgICB4LmZpbGUsXG4gICAgICAgICAgICAgICAgICAgIFVpbnQ4VG9CYXNlNjQobmV3IFVpbnQ4QXJyYXkoeC5ieXRlcykpLFxuICAgICAgICAgICAgICAgIF0pKSxcbiAgICAgICAgICAgIH07XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHt9O1xuICAgIH1cbn1cbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWFIUjBjQzF3YjNOMExYQmhjbk5sY2k1cWN5SXNJbk52ZFhKalpWSnZiM1FpT2lJaUxDSnpiM1Z5WTJWeklqcGJJaTR1THk0dUx5NHVMM055WXk5c2FXSXZhSFIwY0Mxd2IzTjBMWEJoY25ObGNpNTBjeUpkTENKdVlXMWxjeUk2VzEwc0ltMWhjSEJwYm1keklqb2lRVUZEUVN4UFFVRlBMRVZCUVVVc1dVRkJXU3hGUVVGRkxHRkJRV0VzUlVGQlJTeE5RVUZOTEdkQ1FVRm5RaXhEUVVGRE8wRkJVVGRFTEUxQlFVMHNUMEZCVHl4alFVRmpPMGxCUTFJc01rSkJRVEpDTEVOQlFYZERPMGxCUTI1RkxGbEJRVmtzUTBGQlF6dEpRVVU1UWl4WlFVTkZMREpDUVVGclJTeEZRVU5zUlN4WlFVRlpPMUZCUlZvc1NVRkJTU3hEUVVGRExESkNRVUV5UWl4SFFVRkhMREpDUVVFeVFpeERRVUZETzFGQlF5OUVMRWxCUVVrc1EwRkJReXhaUVVGWkxFZEJRVWNzV1VGQldTeERRVUZETzBsQlEyNURMRU5CUVVNN1NVRkZUU3huUWtGQlowSTdVVUZEY2tJc1RVRkJUU3hYUVVGWExFZEJRVWNzU1VGQlNTeERRVUZETERKQ1FVRXlRaXhEUVVGRExGZEJRVmNzUTBGQlF6dFJRVU5xUlN4SlFVRkpMRmRCUVZjc1EwRkJReXhMUVVGTExFVkJRVVU3V1VGRGNrSXNTVUZCU1N4RFFVRkRMRmxCUVZrc1EwRkJReXhSUVVGUkxFTkJRM2hDTERSRFFVRTBReXhIUVVGSExGZEJRVmNzUTBGQlF5eExRVUZMTEVOQlEycEZMRU5CUVVNN1UwRkRTRHRSUVVORUxFbEJRVWtzVjBGQlZ5eERRVUZETEZGQlFWRXNSVUZCUlR0WlFVTjRRaXhQUVVGUE8yZENRVU5NTEZOQlFWTXNSVUZCUlN4WlFVRlpMRU5CUVVNc1NVRkJTU3hEUVVGRExGTkJRVk1zUTBGQlF5eFhRVUZYTEVOQlFVTXNVVUZCVVN4RFFVRkRMRU5CUVVNN1lVRkRPVVFzUTBGQlF6dFRRVU5JTzFGQlEwUXNTVUZCU1N4WFFVRlhMRU5CUVVNc1IwRkJSeXhGUVVGRk8xbEJRMjVDTEU5QlFVODdaMEpCUTB3c1lVRkJZU3hGUVVGRkxFbEJRVWtzUTBGQlF5eFRRVUZUTEVOQlF6TkNMRmRCUVZjc1EwRkJReXhIUVVGSExFTkJRVU1zUjBGQlJ5eERRVUZETEVOQlFVTXNRMEZCUXl4RlFVRkZMRVZCUVVVc1EwRkJRenR2UWtGRGVrSXNRMEZCUXl4RFFVRkRMRWxCUVVrN2IwSkJRMDRzWVVGQllTeERRVUZETEVsQlFVa3NWVUZCVlN4RFFVRkRMRU5CUVVNc1EwRkJReXhMUVVGTExFTkJRVU1zUTBGQlF6dHBRa0ZEZGtNc1EwRkJReXhEUVVOSU8yRkJRMFlzUTBGQlF6dFRRVU5JTzFGQlEwUXNUMEZCVHl4RlFVRkZMRU5CUVVNN1NVRkRXaXhEUVVGRE8wTkJRMFlpZlE9PSIsIi8vIEludHJ1bWVudGF0aW9uIGluamVjdGlvbiBjb2RlIGlzIGJhc2VkIG9uIHByaXZhY3liYWRnZXJmaXJlZm94XG4vLyBodHRwczovL2dpdGh1Yi5jb20vRUZGb3JnL3ByaXZhY3liYWRnZXJmaXJlZm94L2Jsb2IvbWFzdGVyL2RhdGEvZmluZ2VycHJpbnRpbmcuanNcbmV4cG9ydCBmdW5jdGlvbiBnZXRJbnN0cnVtZW50SlMoZXZlbnRJZCwgc2VuZE1lc3NhZ2VzVG9Mb2dnZXIpIHtcbiAgICAvKlxuICAgICAqIEluc3RydW1lbnRhdGlvbiBoZWxwZXJzXG4gICAgICogKElubGluZWQgaW4gb3JkZXIgZm9yIGpzSW5zdHJ1bWVudHMgdG8gYmUgZWFzaWx5IGV4cG9ydGFibGUgYXMgYSBzdHJpbmcpXG4gICAgICovXG4gICAgLy8gZnVuY3Rpb24gZ2V0QXR0cmlidXRlcyAoYXR0cmlidXRlcykge1xuICAgIC8vICAgICByZXR1cm4gQXJyYXkuZnJvbShhdHRyaWJ1dGVzKVxuICAgIC8vICAgICAgIC5tYXAoYSA9PiBbYS5uYW1lLCBhLnZhbHVlXSlcbiAgICAvLyAgICAgICAucmVkdWNlKChhY2MsIGF0dHIpID0+IHtcbiAgICAvLyAgICAgICAgIGFjY1thdHRyWzBdXSA9IGF0dHJbMV1cbiAgICAvLyAgICAgICAgIHJldHVybiBhY2NcbiAgICAvLyAgICAgICB9LCB7fSlcbiAgICAvLyAgIH1cbiAgICAvLyBDb3VudGVyIHRvIGNhcCAjIG9mIGNhbGxzIGxvZ2dlZCBmb3IgZWFjaCBzY3JpcHQvYXBpIGNvbWJpbmF0aW9uXG4gICAgY29uc3QgbWF4TG9nQ291bnQgPSA1MDA7XG4gICAgLy8gbG9nQ291bnRlclxuICAgIGNvbnN0IGxvZ0NvdW50ZXIgPSBuZXcgT2JqZWN0KCk7XG4gICAgLy8gUHJldmVudCBsb2dnaW5nIG9mIGdldHMgYXJpc2luZyBmcm9tIGxvZ2dpbmdcbiAgICBsZXQgaW5Mb2cgPSBmYWxzZTtcbiAgICAvLyBUbyBrZWVwIHRyYWNrIG9mIHRoZSBvcmlnaW5hbCBvcmRlciBvZiBldmVudHNcbiAgICBsZXQgb3JkaW5hbCA9IDA7XG4gICAgLy8gT3B0aW9ucyBmb3IgSlNPcGVyYXRpb25cbiAgICBjb25zdCBKU09wZXJhdGlvbiA9IHtcbiAgICAgICAgY2FsbDogXCJjYWxsXCIsXG4gICAgICAgIGdldDogXCJnZXRcIixcbiAgICAgICAgZ2V0X2ZhaWxlZDogXCJnZXQoZmFpbGVkKVwiLFxuICAgICAgICBnZXRfZnVuY3Rpb246IFwiZ2V0KGZ1bmN0aW9uKVwiLFxuICAgICAgICBzZXQ6IFwic2V0XCIsXG4gICAgICAgIHNldF9mYWlsZWQ6IFwic2V0KGZhaWxlZClcIixcbiAgICAgICAgc2V0X3ByZXZlbnRlZDogXCJzZXQocHJldmVudGVkKVwiLFxuICAgIH07XG4gICAgLy8gUm91Z2ggaW1wbGVtZW50YXRpb25zIG9mIE9iamVjdC5nZXRQcm9wZXJ0eURlc2NyaXB0b3IgYW5kIE9iamVjdC5nZXRQcm9wZXJ0eU5hbWVzXG4gICAgLy8gU2VlIGh0dHA6Ly93aWtpLmVjbWFzY3JpcHQub3JnL2Rva3UucGhwP2lkPWhhcm1vbnk6ZXh0ZW5kZWRfb2JqZWN0X2FwaVxuICAgIE9iamVjdC5nZXRQcm9wZXJ0eURlc2NyaXB0b3IgPSBmdW5jdGlvbiAoc3ViamVjdCwgbmFtZSkge1xuICAgICAgICBpZiAoc3ViamVjdCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoXCJDYW4ndCBnZXQgcHJvcGVydHkgZGVzY3JpcHRvciBmb3IgdW5kZWZpbmVkXCIpO1xuICAgICAgICB9XG4gICAgICAgIGxldCBwZCA9IE9iamVjdC5nZXRPd25Qcm9wZXJ0eURlc2NyaXB0b3Ioc3ViamVjdCwgbmFtZSk7XG4gICAgICAgIGxldCBwcm90byA9IE9iamVjdC5nZXRQcm90b3R5cGVPZihzdWJqZWN0KTtcbiAgICAgICAgd2hpbGUgKHBkID09PSB1bmRlZmluZWQgJiYgcHJvdG8gIT09IG51bGwpIHtcbiAgICAgICAgICAgIHBkID0gT2JqZWN0LmdldE93blByb3BlcnR5RGVzY3JpcHRvcihwcm90bywgbmFtZSk7XG4gICAgICAgICAgICBwcm90byA9IE9iamVjdC5nZXRQcm90b3R5cGVPZihwcm90byk7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHBkO1xuICAgIH07XG4gICAgT2JqZWN0LmdldFByb3BlcnR5TmFtZXMgPSBmdW5jdGlvbiAoc3ViamVjdCkge1xuICAgICAgICBpZiAoc3ViamVjdCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoXCJDYW4ndCBnZXQgcHJvcGVydHkgbmFtZXMgZm9yIHVuZGVmaW5lZFwiKTtcbiAgICAgICAgfVxuICAgICAgICBsZXQgcHJvcHMgPSBPYmplY3QuZ2V0T3duUHJvcGVydHlOYW1lcyhzdWJqZWN0KTtcbiAgICAgICAgbGV0IHByb3RvID0gT2JqZWN0LmdldFByb3RvdHlwZU9mKHN1YmplY3QpO1xuICAgICAgICB3aGlsZSAocHJvdG8gIT09IG51bGwpIHtcbiAgICAgICAgICAgIHByb3BzID0gcHJvcHMuY29uY2F0KE9iamVjdC5nZXRPd25Qcm9wZXJ0eU5hbWVzKHByb3RvKSk7XG4gICAgICAgICAgICBwcm90byA9IE9iamVjdC5nZXRQcm90b3R5cGVPZihwcm90byk7XG4gICAgICAgIH1cbiAgICAgICAgLy8gRklYTUU6IHJlbW92ZSBkdXBsaWNhdGUgcHJvcGVydHkgbmFtZXMgZnJvbSBwcm9wc1xuICAgICAgICByZXR1cm4gcHJvcHM7XG4gICAgfTtcbiAgICAvLyBkZWJvdW5jZSAtIGZyb20gVW5kZXJzY29yZSB2MS42LjBcbiAgICBmdW5jdGlvbiBkZWJvdW5jZShmdW5jLCB3YWl0LCBpbW1lZGlhdGUgPSBmYWxzZSkge1xuICAgICAgICBsZXQgdGltZW91dDtcbiAgICAgICAgbGV0IGFyZ3M7XG4gICAgICAgIGxldCBjb250ZXh0O1xuICAgICAgICBsZXQgdGltZXN0YW1wO1xuICAgICAgICBsZXQgcmVzdWx0O1xuICAgICAgICBjb25zdCBsYXRlciA9IGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgIGNvbnN0IGxhc3QgPSBEYXRlLm5vdygpIC0gdGltZXN0YW1wO1xuICAgICAgICAgICAgaWYgKGxhc3QgPCB3YWl0KSB7XG4gICAgICAgICAgICAgICAgdGltZW91dCA9IHNldFRpbWVvdXQobGF0ZXIsIHdhaXQgLSBsYXN0KTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGVsc2Uge1xuICAgICAgICAgICAgICAgIHRpbWVvdXQgPSBudWxsO1xuICAgICAgICAgICAgICAgIGlmICghaW1tZWRpYXRlKSB7XG4gICAgICAgICAgICAgICAgICAgIHJlc3VsdCA9IGZ1bmMuYXBwbHkoY29udGV4dCwgYXJncyk7XG4gICAgICAgICAgICAgICAgICAgIGNvbnRleHQgPSBhcmdzID0gbnVsbDtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9XG4gICAgICAgIH07XG4gICAgICAgIHJldHVybiBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICBjb250ZXh0ID0gdGhpcztcbiAgICAgICAgICAgIGFyZ3MgPSBhcmd1bWVudHM7XG4gICAgICAgICAgICB0aW1lc3RhbXAgPSBEYXRlLm5vdygpO1xuICAgICAgICAgICAgY29uc3QgY2FsbE5vdyA9IGltbWVkaWF0ZSAmJiAhdGltZW91dDtcbiAgICAgICAgICAgIGlmICghdGltZW91dCkge1xuICAgICAgICAgICAgICAgIHRpbWVvdXQgPSBzZXRUaW1lb3V0KGxhdGVyLCB3YWl0KTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGlmIChjYWxsTm93KSB7XG4gICAgICAgICAgICAgICAgcmVzdWx0ID0gZnVuYy5hcHBseShjb250ZXh0LCBhcmdzKTtcbiAgICAgICAgICAgICAgICBjb250ZXh0ID0gYXJncyA9IG51bGw7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICByZXR1cm4gcmVzdWx0O1xuICAgICAgICB9O1xuICAgIH1cbiAgICAvLyBSZWN1cnNpdmVseSBnZW5lcmF0ZXMgYSBwYXRoIGZvciBhbiBlbGVtZW50XG4gICAgZnVuY3Rpb24gZ2V0UGF0aFRvRG9tRWxlbWVudChlbGVtZW50LCB2aXNpYmlsaXR5QXR0ciA9IGZhbHNlKSB7XG4gICAgICAgIGlmIChlbGVtZW50ID09PSBkb2N1bWVudC5ib2R5KSB7XG4gICAgICAgICAgICByZXR1cm4gZWxlbWVudC50YWdOYW1lO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbGVtZW50LnBhcmVudE5vZGUgPT09IG51bGwpIHtcbiAgICAgICAgICAgIHJldHVybiBcIk5VTEwvXCIgKyBlbGVtZW50LnRhZ05hbWU7XG4gICAgICAgIH1cbiAgICAgICAgbGV0IHNpYmxpbmdJbmRleCA9IDE7XG4gICAgICAgIGNvbnN0IHNpYmxpbmdzID0gZWxlbWVudC5wYXJlbnROb2RlLmNoaWxkTm9kZXM7XG4gICAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgc2libGluZ3MubGVuZ3RoOyBpKyspIHtcbiAgICAgICAgICAgIGNvbnN0IHNpYmxpbmcgPSBzaWJsaW5nc1tpXTtcbiAgICAgICAgICAgIGlmIChzaWJsaW5nID09PSBlbGVtZW50KSB7XG4gICAgICAgICAgICAgICAgbGV0IHBhdGggPSBnZXRQYXRoVG9Eb21FbGVtZW50KGVsZW1lbnQucGFyZW50Tm9kZSwgdmlzaWJpbGl0eUF0dHIpO1xuICAgICAgICAgICAgICAgIHBhdGggKz0gXCIvXCIgKyBlbGVtZW50LnRhZ05hbWUgKyBcIltcIiArIHNpYmxpbmdJbmRleDtcbiAgICAgICAgICAgICAgICBwYXRoICs9IFwiLFwiICsgZWxlbWVudC5pZDtcbiAgICAgICAgICAgICAgICBwYXRoICs9IFwiLFwiICsgZWxlbWVudC5jbGFzc05hbWU7XG4gICAgICAgICAgICAgICAgaWYgKHZpc2liaWxpdHlBdHRyKSB7XG4gICAgICAgICAgICAgICAgICAgIHBhdGggKz0gXCIsXCIgKyBlbGVtZW50LmhpZGRlbjtcbiAgICAgICAgICAgICAgICAgICAgcGF0aCArPSBcIixcIiArIGVsZW1lbnQuc3R5bGUuZGlzcGxheTtcbiAgICAgICAgICAgICAgICAgICAgcGF0aCArPSBcIixcIiArIGVsZW1lbnQuc3R5bGUudmlzaWJpbGl0eTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgaWYgKGVsZW1lbnQudGFnTmFtZSA9PT0gXCJBXCIpIHtcbiAgICAgICAgICAgICAgICAgICAgcGF0aCArPSBcIixcIiArIGVsZW1lbnQuaHJlZjtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgcGF0aCArPSBcIl1cIjtcbiAgICAgICAgICAgICAgICByZXR1cm4gcGF0aDtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGlmIChzaWJsaW5nLm5vZGVUeXBlID09PSAxICYmIHNpYmxpbmcudGFnTmFtZSA9PT0gZWxlbWVudC50YWdOYW1lKSB7XG4gICAgICAgICAgICAgICAgc2libGluZ0luZGV4Kys7XG4gICAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICB9XG4gICAgLy8gSGVscGVyIGZvciBKU09OaWZ5aW5nIG9iamVjdHNcbiAgICBmdW5jdGlvbiBzZXJpYWxpemVPYmplY3Qob2JqZWN0LCBzdHJpbmdpZnlGdW5jdGlvbnMgPSBmYWxzZSkge1xuICAgICAgICAvLyBIYW5kbGUgcGVybWlzc2lvbnMgZXJyb3JzXG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgICBpZiAob2JqZWN0ID09PSBudWxsKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIFwibnVsbFwiO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgaWYgKHR5cGVvZiBvYmplY3QgPT09IFwiZnVuY3Rpb25cIikge1xuICAgICAgICAgICAgICAgIHJldHVybiBzdHJpbmdpZnlGdW5jdGlvbnMgPyBvYmplY3QudG9TdHJpbmcoKSA6IFwiRlVOQ1RJT05cIjtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGlmICh0eXBlb2Ygb2JqZWN0ICE9PSBcIm9iamVjdFwiKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIG9iamVjdDtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGNvbnN0IHNlZW5PYmplY3RzID0gW107XG4gICAgICAgICAgICByZXR1cm4gSlNPTi5zdHJpbmdpZnkob2JqZWN0LCBmdW5jdGlvbiAoa2V5LCB2YWx1ZSkge1xuICAgICAgICAgICAgICAgIGlmICh2YWx1ZSA9PT0gbnVsbCkge1xuICAgICAgICAgICAgICAgICAgICByZXR1cm4gXCJudWxsXCI7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIGlmICh0eXBlb2YgdmFsdWUgPT09IFwiZnVuY3Rpb25cIikge1xuICAgICAgICAgICAgICAgICAgICByZXR1cm4gc3RyaW5naWZ5RnVuY3Rpb25zID8gdmFsdWUudG9TdHJpbmcoKSA6IFwiRlVOQ1RJT05cIjtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gXCJvYmplY3RcIikge1xuICAgICAgICAgICAgICAgICAgICAvLyBSZW1vdmUgd3JhcHBpbmcgb24gY29udGVudCBvYmplY3RzXG4gICAgICAgICAgICAgICAgICAgIGlmIChcIndyYXBwZWRKU09iamVjdFwiIGluIHZhbHVlKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICB2YWx1ZSA9IHZhbHVlLndyYXBwZWRKU09iamVjdDtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAvLyBTZXJpYWxpemUgRE9NIGVsZW1lbnRzXG4gICAgICAgICAgICAgICAgICAgIGlmICh2YWx1ZSBpbnN0YW5jZW9mIEhUTUxFbGVtZW50KSB7XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gZ2V0UGF0aFRvRG9tRWxlbWVudCh2YWx1ZSk7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgLy8gUHJldmVudCBzZXJpYWxpemF0aW9uIGN5Y2xlc1xuICAgICAgICAgICAgICAgICAgICBpZiAoa2V5ID09PSBcIlwiIHx8IHNlZW5PYmplY3RzLmluZGV4T2YodmFsdWUpIDwgMCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgc2Vlbk9iamVjdHMucHVzaCh2YWx1ZSk7XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gdmFsdWU7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gdHlwZW9mIHZhbHVlO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIHJldHVybiB2YWx1ZTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIGNhdGNoIChlcnJvcikge1xuICAgICAgICAgICAgY29uc29sZS5sb2coXCJPcGVuV1BNOiBTRVJJQUxJWkFUSU9OIEVSUk9SOiBcIiArIGVycm9yKTtcbiAgICAgICAgICAgIHJldHVybiBcIlNFUklBTElaQVRJT04gRVJST1I6IFwiICsgZXJyb3I7XG4gICAgICAgIH1cbiAgICB9XG4gICAgZnVuY3Rpb24gdXBkYXRlQ291bnRlckFuZENoZWNrSWZPdmVyKHNjcmlwdFVybCwgc3ltYm9sKSB7XG4gICAgICAgIGNvbnN0IGtleSA9IHNjcmlwdFVybCArIFwifFwiICsgc3ltYm9sO1xuICAgICAgICBpZiAoa2V5IGluIGxvZ0NvdW50ZXIgJiYgbG9nQ291bnRlcltrZXldID49IG1heExvZ0NvdW50KSB7XG4gICAgICAgICAgICByZXR1cm4gdHJ1ZTtcbiAgICAgICAgfVxuICAgICAgICBlbHNlIGlmICghKGtleSBpbiBsb2dDb3VudGVyKSkge1xuICAgICAgICAgICAgbG9nQ291bnRlcltrZXldID0gMTtcbiAgICAgICAgfVxuICAgICAgICBlbHNlIHtcbiAgICAgICAgICAgIGxvZ0NvdW50ZXJba2V5XSArPSAxO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBmYWxzZTtcbiAgICB9XG4gICAgLy8gRm9yIGdldHMsIHNldHMsIGV0Yy4gb24gYSBzaW5nbGUgdmFsdWVcbiAgICBmdW5jdGlvbiBsb2dWYWx1ZShpbnN0cnVtZW50ZWRWYXJpYWJsZU5hbWUsIHZhbHVlLCBhdHRyaWJ1dGVzLCBvcGVyYXRpb24sIC8vIGZyb20gSlNPcGVyYXRpb24gb2JqZWN0IHBsZWFzZVxuICAgIGNhbGxDb250ZXh0LCBsb2dTZXR0aW5ncykge1xuICAgICAgICBpZiAoaW5Mb2cpIHtcbiAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgfVxuICAgICAgICBpbkxvZyA9IHRydWU7XG4gICAgICAgIGNvbnN0IG92ZXJMaW1pdCA9IHVwZGF0ZUNvdW50ZXJBbmRDaGVja0lmT3ZlcihjYWxsQ29udGV4dC5zY3JpcHRVcmwsIGluc3RydW1lbnRlZFZhcmlhYmxlTmFtZSk7XG4gICAgICAgIGlmIChvdmVyTGltaXQpIHtcbiAgICAgICAgICAgIGluTG9nID0gZmFsc2U7XG4gICAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cbiAgICAgICAgLy9hdHRyaWJ1dGVzIGlzIGEgTmFtZWROb2RlTWFwLiBEb2luZyBhIGNvbnZlcnNpb24sIHRha2luZyBvbmx5IG5hbWVzIGFuZCB2YWx1ZXMuXG4gICAgICAgIC8vRG9pbmcgdGhpcyBvbmx5IGZvciBzcmMgY2FsbHMgZm9yIG5vdy5cbiAgICAgICAgaWYgKGluc3RydW1lbnRlZFZhcmlhYmxlTmFtZS5pbmNsdWRlcygnRWxlbWVudC5zcmMnKSkge1xuICAgICAgICAgICAgdmFyIGNvbXBsZXRlVXJsID0gbmV3IFVSTCh2YWx1ZSwgd2luZG93LmxvY2F0aW9uLmhyZWYpO1xuICAgICAgICAgICAgdmFsdWUgPSBjb21wbGV0ZVVybC5ocmVmO1xuICAgICAgICAgICAgdmFyIG5ld09iamVjdCA9IE9iamVjdC5hc3NpZ24oe30sIEFycmF5LmZyb20oYXR0cmlidXRlcywgKHsgbmFtZSwgdmFsdWUgfSkgPT4gKHsgW25hbWVdOiB2YWx1ZSB9KSkpO1xuICAgICAgICAgICAgYXR0cmlidXRlcyA9IG5ld09iamVjdDtcbiAgICAgICAgfVxuICAgICAgICBjb25zdCBtc2cgPSB7XG4gICAgICAgICAgICBvcGVyYXRpb24sXG4gICAgICAgICAgICBzeW1ib2w6IGluc3RydW1lbnRlZFZhcmlhYmxlTmFtZSxcbiAgICAgICAgICAgIHZhbHVlOiBzZXJpYWxpemVPYmplY3QodmFsdWUsIGxvZ1NldHRpbmdzLmxvZ0Z1bmN0aW9uc0FzU3RyaW5ncyksXG4gICAgICAgICAgICBhdHRyaWJ1dGVzOiBzZXJpYWxpemVPYmplY3QoYXR0cmlidXRlcyksXG4gICAgICAgICAgICBzY3JpcHRVcmw6IGNhbGxDb250ZXh0LnNjcmlwdFVybCxcbiAgICAgICAgICAgIHNjcmlwdExpbmU6IGNhbGxDb250ZXh0LnNjcmlwdExpbmUsXG4gICAgICAgICAgICBzY3JpcHRDb2w6IGNhbGxDb250ZXh0LnNjcmlwdENvbCxcbiAgICAgICAgICAgIGZ1bmNOYW1lOiBjYWxsQ29udGV4dC5mdW5jTmFtZSxcbiAgICAgICAgICAgIHNjcmlwdExvY0V2YWw6IGNhbGxDb250ZXh0LnNjcmlwdExvY0V2YWwsXG4gICAgICAgICAgICBjYWxsU3RhY2s6IGNhbGxDb250ZXh0LmNhbGxTdGFjayxcbiAgICAgICAgICAgIG9yZGluYWw6IG9yZGluYWwrKyxcbiAgICAgICAgfTtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgIHNlbmQoXCJsb2dWYWx1ZVwiLCBtc2cpO1xuICAgICAgICB9XG4gICAgICAgIGNhdGNoIChlcnJvcikge1xuICAgICAgICAgICAgY29uc29sZS5sb2coXCJPcGVuV1BNOiBVbnN1Y2Nlc3NmdWwgdmFsdWUgbG9nIVwiKTtcbiAgICAgICAgICAgIGxvZ0Vycm9yVG9Db25zb2xlKGVycm9yKTtcbiAgICAgICAgfVxuICAgICAgICBpbkxvZyA9IGZhbHNlO1xuICAgIH1cbiAgICAvLyBGb3IgZnVuY3Rpb25zXG4gICAgZnVuY3Rpb24gbG9nQ2FsbChpbnN0cnVtZW50ZWRGdW5jdGlvbk5hbWUsIGFyZ3MsIGF0dHJpYnV0ZXMsIGNhbGxDb250ZXh0LCBsb2dTZXR0aW5ncykge1xuICAgICAgICBpZiAoaW5Mb2cpIHtcbiAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgfVxuICAgICAgICBpbkxvZyA9IHRydWU7XG4gICAgICAgIGNvbnN0IG92ZXJMaW1pdCA9IHVwZGF0ZUNvdW50ZXJBbmRDaGVja0lmT3ZlcihjYWxsQ29udGV4dC5zY3JpcHRVcmwsIGluc3RydW1lbnRlZEZ1bmN0aW9uTmFtZSk7XG4gICAgICAgIGlmIChvdmVyTGltaXQpIHtcbiAgICAgICAgICAgIGluTG9nID0gZmFsc2U7XG4gICAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgIC8vIENvbnZlcnQgc3BlY2lhbCBhcmd1bWVudHMgYXJyYXkgdG8gYSBzdGFuZGFyZCBhcnJheSBmb3IgSlNPTmlmeWluZ1xuICAgICAgICAgICAgY29uc3Qgc2VyaWFsQXJncyA9IFtdO1xuICAgICAgICAgICAgZm9yIChjb25zdCBhcmcgb2YgYXJncykge1xuICAgICAgICAgICAgICAgIHNlcmlhbEFyZ3MucHVzaChzZXJpYWxpemVPYmplY3QoYXJnLCBsb2dTZXR0aW5ncy5sb2dGdW5jdGlvbnNBc1N0cmluZ3MpKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGlmIChpbnN0cnVtZW50ZWRGdW5jdGlvbk5hbWUgPT09ICd3aW5kb3cuZG9jdW1lbnQuY3JlYXRlRWxlbWVudCcpIHtcbiAgICAgICAgICAgICAgICB2YXIgbmV3T2JqZWN0ID0gT2JqZWN0LmFzc2lnbih7fSwgQXJyYXkuZnJvbShhdHRyaWJ1dGVzLCAoeyBuYW1lLCB2YWx1ZSB9KSA9PiAoeyBbbmFtZV06IHZhbHVlIH0pKSk7XG4gICAgICAgICAgICAgICAgYXR0cmlidXRlcyA9IG5ld09iamVjdDtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGNvbnN0IG1zZyA9IHtcbiAgICAgICAgICAgICAgICBvcGVyYXRpb246IEpTT3BlcmF0aW9uLmNhbGwsXG4gICAgICAgICAgICAgICAgc3ltYm9sOiBpbnN0cnVtZW50ZWRGdW5jdGlvbk5hbWUsXG4gICAgICAgICAgICAgICAgYXJnczogc2VyaWFsQXJncyxcbiAgICAgICAgICAgICAgICB2YWx1ZTogXCJcIixcbiAgICAgICAgICAgICAgICBhdHRyaWJ1dGVzOiBzZXJpYWxpemVPYmplY3QoYXR0cmlidXRlcyksXG4gICAgICAgICAgICAgICAgc2NyaXB0VXJsOiBjYWxsQ29udGV4dC5zY3JpcHRVcmwsXG4gICAgICAgICAgICAgICAgc2NyaXB0TGluZTogY2FsbENvbnRleHQuc2NyaXB0TGluZSxcbiAgICAgICAgICAgICAgICBzY3JpcHRDb2w6IGNhbGxDb250ZXh0LnNjcmlwdENvbCxcbiAgICAgICAgICAgICAgICBmdW5jTmFtZTogY2FsbENvbnRleHQuZnVuY05hbWUsXG4gICAgICAgICAgICAgICAgc2NyaXB0TG9jRXZhbDogY2FsbENvbnRleHQuc2NyaXB0TG9jRXZhbCxcbiAgICAgICAgICAgICAgICBjYWxsU3RhY2s6IGNhbGxDb250ZXh0LmNhbGxTdGFjayxcbiAgICAgICAgICAgICAgICBvcmRpbmFsOiBvcmRpbmFsKyssXG4gICAgICAgICAgICB9O1xuICAgICAgICAgICAgc2VuZChcImxvZ0NhbGxcIiwgbXNnKTtcbiAgICAgICAgfVxuICAgICAgICBjYXRjaCAoZXJyb3IpIHtcbiAgICAgICAgICAgIGNvbnNvbGUubG9nKFwiT3BlbldQTTogVW5zdWNjZXNzZnVsIGNhbGwgbG9nOiBcIiArIGluc3RydW1lbnRlZEZ1bmN0aW9uTmFtZSk7XG4gICAgICAgICAgICBsb2dFcnJvclRvQ29uc29sZShlcnJvcik7XG4gICAgICAgIH1cbiAgICAgICAgaW5Mb2cgPSBmYWxzZTtcbiAgICB9XG4gICAgZnVuY3Rpb24gbG9nRXJyb3JUb0NvbnNvbGUoZXJyb3IsIGNvbnRleHQgPSBmYWxzZSkge1xuICAgICAgICBjb25zb2xlLmVycm9yKFwiT3BlbldQTTogRXJyb3IgbmFtZTogXCIgKyBlcnJvci5uYW1lKTtcbiAgICAgICAgY29uc29sZS5lcnJvcihcIk9wZW5XUE06IEVycm9yIG1lc3NhZ2U6IFwiICsgZXJyb3IubWVzc2FnZSk7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoXCJPcGVuV1BNOiBFcnJvciBmaWxlbmFtZTogXCIgKyBlcnJvci5maWxlTmFtZSk7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoXCJPcGVuV1BNOiBFcnJvciBsaW5lIG51bWJlcjogXCIgKyBlcnJvci5saW5lTnVtYmVyKTtcbiAgICAgICAgY29uc29sZS5lcnJvcihcIk9wZW5XUE06IEVycm9yIHN0YWNrOiBcIiArIGVycm9yLnN0YWNrKTtcbiAgICAgICAgaWYgKGNvbnRleHQpIHtcbiAgICAgICAgICAgIGNvbnNvbGUuZXJyb3IoXCJPcGVuV1BNOiBFcnJvciBjb250ZXh0OiBcIiArIEpTT04uc3RyaW5naWZ5KGNvbnRleHQpKTtcbiAgICAgICAgfVxuICAgIH1cbiAgICAvLyBIZWxwZXIgdG8gZ2V0IG9yaWdpbmF0aW5nIHNjcmlwdCB1cmxzXG4gICAgZnVuY3Rpb24gZ2V0U3RhY2tUcmFjZSgpIHtcbiAgICAgICAgbGV0IHN0YWNrO1xuICAgICAgICB0cnkge1xuICAgICAgICAgICAgdGhyb3cgbmV3IEVycm9yKCk7XG4gICAgICAgIH1cbiAgICAgICAgY2F0Y2ggKGVycikge1xuICAgICAgICAgICAgc3RhY2sgPSBlcnIuc3RhY2s7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHN0YWNrO1xuICAgIH1cbiAgICAvLyBmcm9tIGh0dHA6Ly9zdGFja292ZXJmbG93LmNvbS9hLzUyMDIxODVcbiAgICBjb25zdCByc3BsaXQgPSBmdW5jdGlvbiAoc291cmNlLCBzZXAsIG1heHNwbGl0KSB7XG4gICAgICAgIGNvbnN0IHNwbGl0ID0gc291cmNlLnNwbGl0KHNlcCk7XG4gICAgICAgIHJldHVybiBtYXhzcGxpdFxuICAgICAgICAgICAgPyBbc3BsaXQuc2xpY2UoMCwgLW1heHNwbGl0KS5qb2luKHNlcCldLmNvbmNhdChzcGxpdC5zbGljZSgtbWF4c3BsaXQpKVxuICAgICAgICAgICAgOiBzcGxpdDtcbiAgICB9O1xuICAgIGZ1bmN0aW9uIGdldE9yaWdpbmF0aW5nU2NyaXB0Q29udGV4dChnZXRDYWxsU3RhY2sgPSBmYWxzZSkge1xuICAgICAgICBjb25zdCB0cmFjZSA9IGdldFN0YWNrVHJhY2UoKS50cmltKCkuc3BsaXQoXCJcXG5cIik7XG4gICAgICAgIC8vIHJldHVybiBhIGNvbnRleHQgb2JqZWN0IGV2ZW4gaWYgdGhlcmUgaXMgYW4gZXJyb3JcbiAgICAgICAgY29uc3QgZW1wdHlfY29udGV4dCA9IHtcbiAgICAgICAgICAgIHNjcmlwdFVybDogXCJcIixcbiAgICAgICAgICAgIHNjcmlwdExpbmU6IFwiXCIsXG4gICAgICAgICAgICBzY3JpcHRDb2w6IFwiXCIsXG4gICAgICAgICAgICBmdW5jTmFtZTogXCJcIixcbiAgICAgICAgICAgIHNjcmlwdExvY0V2YWw6IFwiXCIsXG4gICAgICAgICAgICBjYWxsU3RhY2s6IFwiXCIsXG4gICAgICAgIH07XG4gICAgICAgIGlmICh0cmFjZS5sZW5ndGggPCA0KSB7XG4gICAgICAgICAgICByZXR1cm4gZW1wdHlfY29udGV4dDtcbiAgICAgICAgfVxuICAgICAgICAvLyAwLCAxIGFuZCAyIGFyZSBPcGVuV1BNJ3Mgb3duIGZ1bmN0aW9ucyAoZS5nLiBnZXRTdGFja1RyYWNlKSwgc2tpcCB0aGVtLlxuICAgICAgICBjb25zdCBjYWxsU2l0ZSA9IHRyYWNlWzNdO1xuICAgICAgICBpZiAoIWNhbGxTaXRlKSB7XG4gICAgICAgICAgICByZXR1cm4gZW1wdHlfY29udGV4dDtcbiAgICAgICAgfVxuICAgICAgICAvKlxuICAgICAgICAgKiBTdGFjayBmcmFtZSBmb3JtYXQgaXMgc2ltcGx5OiBGVU5DX05BTUVARklMRU5BTUU6TElORV9OTzpDT0xVTU5fTk9cbiAgICAgICAgICpcbiAgICAgICAgICogSWYgZXZhbCBvciBGdW5jdGlvbiBpcyBpbnZvbHZlZCB3ZSBoYXZlIGFuIGFkZGl0aW9uYWwgcGFydCBhZnRlciB0aGUgRklMRU5BTUUsIGUuZy46XG4gICAgICAgICAqIEZVTkNfTkFNRUBGSUxFTkFNRSBsaW5lIDEyMyA+IGV2YWwgbGluZSAxID4gZXZhbDpMSU5FX05POkNPTFVNTl9OT1xuICAgICAgICAgKiBvciBGVU5DX05BTUVARklMRU5BTUUgbGluZSAyMzQgPiBGdW5jdGlvbjpMSU5FX05POkNPTFVNTl9OT1xuICAgICAgICAgKlxuICAgICAgICAgKiBXZSBzdG9yZSB0aGUgcGFydCBiZXR3ZWVuIHRoZSBGSUxFTkFNRSBhbmQgdGhlIExJTkVfTk8gaW4gc2NyaXB0TG9jRXZhbFxuICAgICAgICAgKi9cbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgIGxldCBzY3JpcHRVcmwgPSBcIlwiO1xuICAgICAgICAgICAgbGV0IHNjcmlwdExvY0V2YWwgPSBcIlwiOyAvLyBmb3IgZXZhbCBvciBGdW5jdGlvbiBjYWxsc1xuICAgICAgICAgICAgY29uc3QgY2FsbFNpdGVQYXJ0cyA9IGNhbGxTaXRlLnNwbGl0KFwiQFwiKTtcbiAgICAgICAgICAgIGNvbnN0IGZ1bmNOYW1lID0gY2FsbFNpdGVQYXJ0c1swXSB8fCBcIlwiO1xuICAgICAgICAgICAgY29uc3QgaXRlbXMgPSByc3BsaXQoY2FsbFNpdGVQYXJ0c1sxXSwgXCI6XCIsIDIpO1xuICAgICAgICAgICAgY29uc3QgY29sdW1uTm8gPSBpdGVtc1tpdGVtcy5sZW5ndGggLSAxXTtcbiAgICAgICAgICAgIGNvbnN0IGxpbmVObyA9IGl0ZW1zW2l0ZW1zLmxlbmd0aCAtIDJdO1xuICAgICAgICAgICAgY29uc3Qgc2NyaXB0RmlsZU5hbWUgPSBpdGVtc1tpdGVtcy5sZW5ndGggLSAzXSB8fCBcIlwiO1xuICAgICAgICAgICAgY29uc3QgbGluZU5vSWR4ID0gc2NyaXB0RmlsZU5hbWUuaW5kZXhPZihcIiBsaW5lIFwiKTsgLy8gbGluZSBpbiB0aGUgVVJMIG1lYW5zIGV2YWwgb3IgRnVuY3Rpb25cbiAgICAgICAgICAgIGlmIChsaW5lTm9JZHggPT09IC0xKSB7XG4gICAgICAgICAgICAgICAgc2NyaXB0VXJsID0gc2NyaXB0RmlsZU5hbWU7IC8vIFRPRE86IHNvbWV0aW1lcyB3ZSBoYXZlIGZpbGVuYW1lIG9ubHksIGUuZy4gWFguanNcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGVsc2Uge1xuICAgICAgICAgICAgICAgIHNjcmlwdFVybCA9IHNjcmlwdEZpbGVOYW1lLnNsaWNlKDAsIGxpbmVOb0lkeCk7XG4gICAgICAgICAgICAgICAgc2NyaXB0TG9jRXZhbCA9IHNjcmlwdEZpbGVOYW1lLnNsaWNlKGxpbmVOb0lkeCArIDEsIHNjcmlwdEZpbGVOYW1lLmxlbmd0aCk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBjb25zdCBjYWxsQ29udGV4dCA9IHtcbiAgICAgICAgICAgICAgICBzY3JpcHRVcmwsXG4gICAgICAgICAgICAgICAgc2NyaXB0TGluZTogbGluZU5vLFxuICAgICAgICAgICAgICAgIHNjcmlwdENvbDogY29sdW1uTm8sXG4gICAgICAgICAgICAgICAgZnVuY05hbWUsXG4gICAgICAgICAgICAgICAgc2NyaXB0TG9jRXZhbCxcbiAgICAgICAgICAgICAgICBjYWxsU3RhY2s6IGdldENhbGxTdGFjayA/IHRyYWNlLnNsaWNlKDMpLmpvaW4oXCJcXG5cIikudHJpbSgpIDogXCJcIixcbiAgICAgICAgICAgIH07XG4gICAgICAgICAgICByZXR1cm4gY2FsbENvbnRleHQ7XG4gICAgICAgIH1cbiAgICAgICAgY2F0Y2ggKGUpIHtcbiAgICAgICAgICAgIGNvbnNvbGUubG9nKFwiT3BlbldQTTogRXJyb3IgcGFyc2luZyB0aGUgc2NyaXB0IGNvbnRleHRcIiwgZS50b1N0cmluZygpLCBjYWxsU2l0ZSk7XG4gICAgICAgICAgICByZXR1cm4gZW1wdHlfY29udGV4dDtcbiAgICAgICAgfVxuICAgIH1cbiAgICBmdW5jdGlvbiBpc09iamVjdChvYmplY3QsIHByb3BlcnR5TmFtZSkge1xuICAgICAgICBsZXQgcHJvcGVydHk7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgICBwcm9wZXJ0eSA9IG9iamVjdFtwcm9wZXJ0eU5hbWVdO1xuICAgICAgICB9XG4gICAgICAgIGNhdGNoIChlcnJvcikge1xuICAgICAgICAgICAgcmV0dXJuIGZhbHNlO1xuICAgICAgICB9XG4gICAgICAgIGlmIChwcm9wZXJ0eSA9PT0gbnVsbCkge1xuICAgICAgICAgICAgLy8gbnVsbCBpcyB0eXBlIFwib2JqZWN0XCJcbiAgICAgICAgICAgIHJldHVybiBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gdHlwZW9mIHByb3BlcnR5ID09PSBcIm9iamVjdFwiO1xuICAgIH1cbiAgICAvLyBMb2cgY2FsbHMgdG8gYSBnaXZlbiBmdW5jdGlvblxuICAgIC8vIFRoaXMgaGVscGVyIGZ1bmN0aW9uIHJldHVybnMgYSB3cmFwcGVyIGFyb3VuZCBgZnVuY2Agd2hpY2ggbG9ncyBjYWxsc1xuICAgIC8vIHRvIGBmdW5jYC4gYG9iamVjdE5hbWVgIGFuZCBgbWV0aG9kTmFtZWAgYXJlIHVzZWQgc3RyaWN0bHkgdG8gaWRlbnRpZnlcbiAgICAvLyB3aGljaCBvYmplY3QgbWV0aG9kIGBmdW5jYCBpcyBjb21pbmcgZnJvbSBpbiB0aGUgbG9nc1xuICAgIGZ1bmN0aW9uIGluc3RydW1lbnRGdW5jdGlvbihvYmplY3ROYW1lLCBtZXRob2ROYW1lLCBmdW5jLCBsb2dTZXR0aW5ncykge1xuICAgICAgICByZXR1cm4gZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgY29uc3QgY2FsbENvbnRleHQgPSBnZXRPcmlnaW5hdGluZ1NjcmlwdENvbnRleHQobG9nU2V0dGluZ3MubG9nQ2FsbFN0YWNrKTtcbiAgICAgICAgICAgIC8qKioqKioqKioqKioqKioqKioqKiovXG4gICAgICAgICAgICB2YXIgYXR0cmlidXRlcyA9IFwiXCI7XG4gICAgICAgICAgICAvLyBDaGVjayBmb3IgY3JlYXRpb24gb2YgZWxlbWVudC4gU2V0IGFuIG9wZW53cG0gYXR0cmlidXRlIHRvIGlkZW50aWZ5IHRoaXNcbiAgICAgICAgICAgIC8vIGVsZW1lbnQgbGF0ZXIuIFxuICAgICAgICAgICAgaWYgKG1ldGhvZE5hbWUgPT0gXCJjcmVhdGVFbGVtZW50XCIpIHtcbiAgICAgICAgICAgICAgICAvL3ZhciBvYnNlcnZlciA9IGNyZWF0ZU11dGF0aW9uT2JzZXJ2ZXIob2JqZWN0LCBvYmplY3ROYW1lLCBsb2dTZXR0aW5ncyk7XG4gICAgICAgICAgICAgICAgLy8gb2JqZWN0LmFkZEV2ZW50TGlzdGVuZXIoXCJET01BdHRyTW9kaWZpZWRcIiwgZnVuY3Rpb24gKGV2ZW50KSB7XG4gICAgICAgICAgICAgICAgLy8gY29uc29sZS5sb2coXCJpbiBtb2RpZnkgbGlzdGVuZXIhXCIpO1xuICAgICAgICAgICAgICAgIC8vIHN3aXRjaChldmVudC5hdHRyQ2hhbmdlKSB7XG4gICAgICAgICAgICAgICAgLy8gICBjYXNlIE11dGF0aW9uRXZlbnQuTU9ESUZJQ0FUSU9OOlxuICAgICAgICAgICAgICAgIC8vICAgICBjb25zb2xlLmxvZyhcIm1vZGlmaWVkIVwiKTtcbiAgICAgICAgICAgICAgICAvLyAgICAgdmFyIG5vZGUgPSBldmVudC50YXJnZXQudGFnTmFtZTtcbiAgICAgICAgICAgICAgICAvLyAgICAgbG9nQ2FsbChub2RlICsgXCIuYXR0cl9tb2RpZmllZFwiLCBcIlwiLCBldmVudC50YXJnZXQuYXR0cmlidXRlcywgY2FsbENvbnRleHQsIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgICAgICAvLyAgICAgYnJlYWs7XG4gICAgICAgICAgICAgICAgLy8gICBjYXNlIE11dGF0aW9uRXZlbnQuQURESVRJT046XG4gICAgICAgICAgICAgICAgLy8gICAgIGNvbnNvbGUubG9nKFwiYWRkZWQhXCIpO1xuICAgICAgICAgICAgICAgIC8vICAgICBsb2dDYWxsKG5vZGUgKyBcIi5hdHRyX2FkZGVkXCIsIFwiXCIsIGV2ZW50LnRhcmdldC5hdHRyaWJ1dGVzLCBjYWxsQ29udGV4dCwgbG9nU2V0dGluZ3MpO1xuICAgICAgICAgICAgICAgIC8vICAgICBicmVhaztcbiAgICAgICAgICAgICAgICAvLyAgIGNhc2UgTXV0YXRpb25FdmVudC5SRU1PVkFMOlxuICAgICAgICAgICAgICAgIC8vICAgICBjb25zb2xlLmxvZyhcInJlbW92ZWQhXCIpO1xuICAgICAgICAgICAgICAgIC8vICAgICBsb2dDYWxsKG5vZGUgKyBcIi5hdHRyX3JlbW92ZWRcIiwgXCJcIiwgZXZlbnQudGFyZ2V0LmF0dHJpYnV0ZXMsIGNhbGxDb250ZXh0LCBsb2dTZXR0aW5ncyk7XG4gICAgICAgICAgICAgICAgLy8gICAgIGJyZWFrO1xuICAgICAgICAgICAgICAgIC8vICAgfVxuICAgICAgICAgICAgICAgIC8vIH0sIGZhbHNlKTtcbiAgICAgICAgICAgICAgICB2YXIgZnVuY1JlZiA9IGZ1bmMuYXBwbHkodGhpcywgYXJndW1lbnRzKTtcbiAgICAgICAgICAgICAgICAvL1NldHRpbmcgYSByYW5kb20gNS1kaWdpdCB0YWcgZm9yIG5vdy5cbiAgICAgICAgICAgICAgICB2YXIgdGFnID0gTWF0aC5mbG9vcihNYXRoLnJhbmRvbSgpICogTWF0aC5wb3coMTAsIDUpKTtcbiAgICAgICAgICAgICAgICBmdW5jUmVmLnNldEF0dHJpYnV0ZShcIm9wZW53cG1cIiwgdGFnKTtcbiAgICAgICAgICAgICAgICBhdHRyaWJ1dGVzID0gZnVuY1JlZi5hdHRyaWJ1dGVzO1xuICAgICAgICAgICAgICAgIGxvZ0NhbGwob2JqZWN0TmFtZSArICcuJyArIG1ldGhvZE5hbWUsIGFyZ3VtZW50cywgYXR0cmlidXRlcywgY2FsbENvbnRleHQsIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgICAgICByZXR1cm4gZnVuY1JlZjtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIC8qKioqKioqKioqKioqKioqKioqKiovXG4gICAgICAgICAgICBsb2dDYWxsKG9iamVjdE5hbWUgKyBcIi5cIiArIG1ldGhvZE5hbWUsIGFyZ3VtZW50cywgYXR0cmlidXRlcywgY2FsbENvbnRleHQsIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgIHJldHVybiBmdW5jLmFwcGx5KHRoaXMsIGFyZ3VtZW50cyk7XG4gICAgICAgIH07XG4gICAgfVxuICAgIC8vIExvZyBwcm9wZXJ0aWVzIG9mIHByb3RvdHlwZXMgYW5kIG9iamVjdHNcbiAgICBmdW5jdGlvbiBpbnN0cnVtZW50T2JqZWN0UHJvcGVydHkob2JqZWN0LCBvYmplY3ROYW1lLCBwcm9wZXJ0eU5hbWUsIGxvZ1NldHRpbmdzKSB7XG4gICAgICAgIGlmICghb2JqZWN0IHx8XG4gICAgICAgICAgICAhb2JqZWN0TmFtZSB8fFxuICAgICAgICAgICAgIXByb3BlcnR5TmFtZSB8fFxuICAgICAgICAgICAgcHJvcGVydHlOYW1lID09PSBcInVuZGVmaW5lZFwiKSB7XG4gICAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoYEludmFsaWQgcmVxdWVzdCB0byBpbnN0cnVtZW50T2JqZWN0UHJvcGVydHkuXG4gICAgICAgIE9iamVjdDogJHtvYmplY3R9XG4gICAgICAgIG9iamVjdE5hbWU6ICR7b2JqZWN0TmFtZX1cbiAgICAgICAgcHJvcGVydHlOYW1lOiAke3Byb3BlcnR5TmFtZX1cbiAgICAgICAgYCk7XG4gICAgICAgIH1cbiAgICAgICAgLy8gU3RvcmUgb3JpZ2luYWwgZGVzY3JpcHRvciBpbiBjbG9zdXJlXG4gICAgICAgIGNvbnN0IHByb3BEZXNjID0gT2JqZWN0LmdldFByb3BlcnR5RGVzY3JpcHRvcihvYmplY3QsIHByb3BlcnR5TmFtZSk7XG4gICAgICAgIC8vIFByb3BlcnR5IGRlc2NyaXB0b3IgbXVzdCBleGlzdCB1bmxlc3Mgd2UgYXJlIGluc3RydW1lbnRpbmcgYSBub25FeGlzdGluZyBwcm9wZXJ0eVxuICAgICAgICBpZiAoIXByb3BEZXNjICYmXG4gICAgICAgICAgICAhbG9nU2V0dGluZ3Mubm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50LmluY2x1ZGVzKHByb3BlcnR5TmFtZSkpIHtcbiAgICAgICAgICAgIGNvbnNvbGUuZXJyb3IoXCJQcm9wZXJ0eSBkZXNjcmlwdG9yIG5vdCBmb3VuZCBmb3JcIiwgb2JqZWN0TmFtZSwgcHJvcGVydHlOYW1lLCBvYmplY3QpO1xuICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG4gICAgICAgIC8vIFByb3BlcnR5IGRlc2NyaXB0b3IgZm9yIHVuZGVmaW5lZCBwcm9wZXJ0aWVzXG4gICAgICAgIGxldCB1bmRlZmluZWRQcm9wVmFsdWU7XG4gICAgICAgIGNvbnN0IHVuZGVmaW5lZFByb3BEZXNjID0ge1xuICAgICAgICAgICAgZ2V0OiAoKSA9PiB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIHVuZGVmaW5lZFByb3BWYWx1ZTtcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICBzZXQ6ICh2YWx1ZSkgPT4ge1xuICAgICAgICAgICAgICAgIHVuZGVmaW5lZFByb3BWYWx1ZSA9IHZhbHVlO1xuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIGVudW1lcmFibGU6IGZhbHNlLFxuICAgICAgICB9O1xuICAgICAgICAvLyBJbnN0cnVtZW50IGRhdGEgb3IgYWNjZXNzb3IgcHJvcGVydHkgZGVzY3JpcHRvcnNcbiAgICAgICAgY29uc3Qgb3JpZ2luYWxHZXR0ZXIgPSBwcm9wRGVzYyA/IHByb3BEZXNjLmdldCA6IHVuZGVmaW5lZFByb3BEZXNjLmdldDtcbiAgICAgICAgY29uc3Qgb3JpZ2luYWxTZXR0ZXIgPSBwcm9wRGVzYyA/IHByb3BEZXNjLnNldCA6IHVuZGVmaW5lZFByb3BEZXNjLnNldDtcbiAgICAgICAgbGV0IG9yaWdpbmFsVmFsdWUgPSBwcm9wRGVzYyA/IHByb3BEZXNjLnZhbHVlIDogdW5kZWZpbmVkUHJvcFZhbHVlO1xuICAgICAgICAvLyBXZSBvdmVyd3JpdGUgYm90aCBkYXRhIGFuZCBhY2Nlc3NvciBwcm9wZXJ0aWVzIGFzIGFuIGluc3RydW1lbnRlZFxuICAgICAgICAvLyBhY2Nlc3NvciBwcm9wZXJ0eVxuICAgICAgICBPYmplY3QuZGVmaW5lUHJvcGVydHkob2JqZWN0LCBwcm9wZXJ0eU5hbWUsIHtcbiAgICAgICAgICAgIGNvbmZpZ3VyYWJsZTogdHJ1ZSxcbiAgICAgICAgICAgIGdldDogKGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgICAgICAgICBsZXQgb3JpZ1Byb3BlcnR5O1xuICAgICAgICAgICAgICAgICAgICBjb25zdCBjYWxsQ29udGV4dCA9IGdldE9yaWdpbmF0aW5nU2NyaXB0Q29udGV4dChsb2dTZXR0aW5ncy5sb2dDYWxsU3RhY2spO1xuICAgICAgICAgICAgICAgICAgICBjb25zdCBpbnN0cnVtZW50ZWRWYXJpYWJsZU5hbWUgPSBgJHtvYmplY3ROYW1lfS4ke3Byb3BlcnR5TmFtZX1gO1xuICAgICAgICAgICAgICAgICAgICB2YXIgYXR0cmlidXRlcyA9IFwiXCI7XG4gICAgICAgICAgICAgICAgICAgIC8vIGdldCBvcmlnaW5hbCB2YWx1ZVxuICAgICAgICAgICAgICAgICAgICBpZiAoIXByb3BEZXNjKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAvLyBpZiB1bmRlZmluZWQgcHJvcGVydHlcbiAgICAgICAgICAgICAgICAgICAgICAgIG9yaWdQcm9wZXJ0eSA9IHVuZGVmaW5lZFByb3BWYWx1ZTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICBlbHNlIGlmIChvcmlnaW5hbEdldHRlcikge1xuICAgICAgICAgICAgICAgICAgICAgICAgLy8gaWYgYWNjZXNzb3IgcHJvcGVydHlcbiAgICAgICAgICAgICAgICAgICAgICAgIG9yaWdQcm9wZXJ0eSA9IG9yaWdpbmFsR2V0dGVyLmNhbGwodGhpcyk7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgZWxzZSBpZiAoXCJ2YWx1ZVwiIGluIHByb3BEZXNjKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAvLyBpZiBkYXRhIHByb3BlcnR5XG4gICAgICAgICAgICAgICAgICAgICAgICBvcmlnUHJvcGVydHkgPSBvcmlnaW5hbFZhbHVlO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgIGVsc2Uge1xuICAgICAgICAgICAgICAgICAgICAgICAgY29uc29sZS5lcnJvcihgUHJvcGVydHkgZGVzY3JpcHRvciBmb3IgJHtpbnN0cnVtZW50ZWRWYXJpYWJsZU5hbWV9IGRvZXNuJ3QgaGF2ZSBnZXR0ZXIgb3IgdmFsdWU/YCk7XG4gICAgICAgICAgICAgICAgICAgICAgICBsb2dWYWx1ZShpbnN0cnVtZW50ZWRWYXJpYWJsZU5hbWUsIFwiXCIsIGF0dHJpYnV0ZXMsIEpTT3BlcmF0aW9uLmdldF9mYWlsZWQsIGNhbGxDb250ZXh0LCBsb2dTZXR0aW5ncyk7XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgLy8gTG9nIGBnZXRzYCBleGNlcHQgdGhvc2UgdGhhdCBoYXZlIGluc3RydW1lbnRlZCByZXR1cm4gdmFsdWVzXG4gICAgICAgICAgICAgICAgICAgIC8vICogQWxsIHJldHVybmVkIGZ1bmN0aW9ucyBhcmUgaW5zdHJ1bWVudGVkIHdpdGggYSB3cmFwcGVyXG4gICAgICAgICAgICAgICAgICAgIC8vICogUmV0dXJuZWQgb2JqZWN0cyBtYXkgYmUgaW5zdHJ1bWVudGVkIGlmIHJlY3Vyc2l2ZVxuICAgICAgICAgICAgICAgICAgICAvLyAgIGluc3RydW1lbnRhdGlvbiBpcyBlbmFibGVkIGFuZCB0aGlzIGlzbid0IGF0IHRoZSBkZXB0aCBsaW1pdC5cbiAgICAgICAgICAgICAgICAgICAgaWYgKHR5cGVvZiBvcmlnUHJvcGVydHkgPT09IFwiZnVuY3Rpb25cIikge1xuICAgICAgICAgICAgICAgICAgICAgICAgaWYgKGxvZ1NldHRpbmdzLmxvZ0Z1bmN0aW9uR2V0cykge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGxvZ1ZhbHVlKGluc3RydW1lbnRlZFZhcmlhYmxlTmFtZSwgb3JpZ1Byb3BlcnR5LCBhdHRyaWJ1dGVzLCBKU09wZXJhdGlvbi5nZXRfZnVuY3Rpb24sIGNhbGxDb250ZXh0LCBsb2dTZXR0aW5ncyk7XG4gICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICAgICBjb25zdCBpbnN0cnVtZW50ZWRGdW5jdGlvbldyYXBwZXIgPSBpbnN0cnVtZW50RnVuY3Rpb24ob2JqZWN0TmFtZSwgcHJvcGVydHlOYW1lLCBvcmlnUHJvcGVydHksIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIC8vIFJlc3RvcmUgdGhlIG9yaWdpbmFsIHByb3RvdHlwZSBhbmQgY29uc3RydWN0b3Igc28gdGhhdCBpbnN0cnVtZW50ZWQgY2xhc3NlcyByZW1haW4gaW50YWN0XG4gICAgICAgICAgICAgICAgICAgICAgICAvLyBUT0RPOiBUaGlzIG1heSBoYXZlIGludHJvZHVjZWQgcHJvdG90eXBlIHBvbGx1dGlvbiBhcyBwZXIgaHR0cHM6Ly9naXRodWIuY29tL21vemlsbGEvT3BlbldQTS9pc3N1ZXMvNDcxXG4gICAgICAgICAgICAgICAgICAgICAgICBpZiAob3JpZ1Byb3BlcnR5LnByb3RvdHlwZSkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGluc3RydW1lbnRlZEZ1bmN0aW9uV3JhcHBlci5wcm90b3R5cGUgPSBvcmlnUHJvcGVydHkucHJvdG90eXBlO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmIChvcmlnUHJvcGVydHkucHJvdG90eXBlLmNvbnN0cnVjdG9yKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGluc3RydW1lbnRlZEZ1bmN0aW9uV3JhcHBlci5wcm90b3R5cGUuY29uc3RydWN0b3IgPVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgb3JpZ1Byb3BlcnR5LnByb3RvdHlwZS5jb25zdHJ1Y3RvcjtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gaW5zdHJ1bWVudGVkRnVuY3Rpb25XcmFwcGVyO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgIGVsc2UgaWYgKHR5cGVvZiBvcmlnUHJvcGVydHkgPT09IFwib2JqZWN0XCIgJiZcbiAgICAgICAgICAgICAgICAgICAgICAgIGxvZ1NldHRpbmdzLnJlY3Vyc2l2ZSAmJlxuICAgICAgICAgICAgICAgICAgICAgICAgbG9nU2V0dGluZ3MuZGVwdGggPiAwKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gb3JpZ1Byb3BlcnR5O1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgIGVsc2Uge1xuICAgICAgICAgICAgICAgICAgICAgICAgbG9nVmFsdWUoaW5zdHJ1bWVudGVkVmFyaWFibGVOYW1lLCBvcmlnUHJvcGVydHksIGF0dHJpYnV0ZXMsIEpTT3BlcmF0aW9uLmdldCwgY2FsbENvbnRleHQsIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBvcmlnUHJvcGVydHk7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9O1xuICAgICAgICAgICAgfSkoKSxcbiAgICAgICAgICAgIHNldDogKGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gZnVuY3Rpb24gKHZhbHVlKSB7XG4gICAgICAgICAgICAgICAgICAgIGNvbnN0IGNhbGxDb250ZXh0ID0gZ2V0T3JpZ2luYXRpbmdTY3JpcHRDb250ZXh0KGxvZ1NldHRpbmdzLmxvZ0NhbGxTdGFjayk7XG4gICAgICAgICAgICAgICAgICAgIGNvbnN0IGluc3RydW1lbnRlZFZhcmlhYmxlTmFtZSA9IGAke29iamVjdE5hbWV9LiR7cHJvcGVydHlOYW1lfWA7XG4gICAgICAgICAgICAgICAgICAgIGxldCByZXR1cm5WYWx1ZTtcbiAgICAgICAgICAgICAgICAgICAgdmFyIGF0dHJpYnV0ZXMgPSBcIlwiO1xuICAgICAgICAgICAgICAgICAgICAvLyBQcmV2ZW50IHNldHMgZm9yIGZ1bmN0aW9ucyBhbmQgb2JqZWN0cyBpZiBlbmFibGVkXG4gICAgICAgICAgICAgICAgICAgIGlmIChsb2dTZXR0aW5ncy5wcmV2ZW50U2V0cyAmJlxuICAgICAgICAgICAgICAgICAgICAgICAgKHR5cGVvZiBvcmlnaW5hbFZhbHVlID09PSBcImZ1bmN0aW9uXCIgfHxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB0eXBlb2Ygb3JpZ2luYWxWYWx1ZSA9PT0gXCJvYmplY3RcIikpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGxvZ1ZhbHVlKGluc3RydW1lbnRlZFZhcmlhYmxlTmFtZSwgdmFsdWUsIGF0dHJpYnV0ZXMsIEpTT3BlcmF0aW9uLnNldF9wcmV2ZW50ZWQsIGNhbGxDb250ZXh0LCBsb2dTZXR0aW5ncyk7XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gdmFsdWU7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgLy8gc2V0IG5ldyB2YWx1ZSB0byBvcmlnaW5hbCBzZXR0ZXIvbG9jYXRpb25cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9yaWdpbmFsU2V0dGVyKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAvLyBpZiBhY2Nlc3NvciBwcm9wZXJ0eVxuICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuVmFsdWUgPSBvcmlnaW5hbFNldHRlci5jYWxsKHRoaXMsIHZhbHVlKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIHRyeSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgKHRoaXMuZ2V0QXR0cmlidXRlKFwib3BlbndwbVwiKSkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBhdHRyaWJ1dGVzID0gdGhpcy5hdHRyaWJ1dGVzO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgIGNhdGNoIChlcnJvcikge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbnNvbGUud2FybihcIkVycm9yIGluIGdldHRpbmcgb3BlbndwbSBhdHRyaWJ1dGVcIik7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgYXR0cmlidXRlcyA9IFwiXCI7XG4gICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgZWxzZSBpZiAoXCJ2YWx1ZVwiIGluIHByb3BEZXNjKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBpbkxvZyA9IHRydWU7XG4gICAgICAgICAgICAgICAgICAgICAgICBpZiAob2JqZWN0LmlzUHJvdG90eXBlT2YodGhpcykpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBPYmplY3QuZGVmaW5lUHJvcGVydHkodGhpcywgcHJvcGVydHlOYW1lLCB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHZhbHVlLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgb3JpZ2luYWxWYWx1ZSA9IHZhbHVlO1xuICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuVmFsdWUgPSB2YWx1ZTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGluTG9nID0gZmFsc2U7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBjb25zb2xlLmVycm9yKGBQcm9wZXJ0eSBkZXNjcmlwdG9yIGZvciAke2luc3RydW1lbnRlZFZhcmlhYmxlTmFtZX0gZG9lc24ndCBoYXZlIHNldHRlciBvciB2YWx1ZT9gKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGxvZ1ZhbHVlKGluc3RydW1lbnRlZFZhcmlhYmxlTmFtZSwgdmFsdWUsIGF0dHJpYnV0ZXMsIEpTT3BlcmF0aW9uLnNldF9mYWlsZWQsIGNhbGxDb250ZXh0LCBsb2dTZXR0aW5ncyk7XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gdmFsdWU7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgbG9nVmFsdWUoaW5zdHJ1bWVudGVkVmFyaWFibGVOYW1lLCB2YWx1ZSwgYXR0cmlidXRlcywgSlNPcGVyYXRpb24uc2V0LCBjYWxsQ29udGV4dCwgbG9nU2V0dGluZ3MpO1xuICAgICAgICAgICAgICAgICAgICByZXR1cm4gcmV0dXJuVmFsdWU7XG4gICAgICAgICAgICAgICAgfTtcbiAgICAgICAgICAgIH0pKCksXG4gICAgICAgIH0pO1xuICAgIH1cbiAgICBmdW5jdGlvbiBpbnN0cnVtZW50T2JqZWN0KG9iamVjdCwgaW5zdHJ1bWVudGVkTmFtZSwgbG9nU2V0dGluZ3MpIHtcbiAgICAgICAgLy8gU2V0IHByb3BlcnRpZXNUb0luc3RydW1lbnQgdG8gbnVsbCB0byBmb3JjZSBubyBwcm9wZXJ0aWVzIHRvIGJlIGluc3RydW1lbnRlZC5cbiAgICAgICAgLy8gKHRoaXMgaXMgdXNlZCBpbiB0ZXN0aW5nIGZvciBleGFtcGxlKVxuICAgICAgICBsZXQgcHJvcGVydGllc1RvSW5zdHJ1bWVudDtcbiAgICAgICAgaWYgKGxvZ1NldHRpbmdzLnByb3BlcnRpZXNUb0luc3RydW1lbnQgPT09IG51bGwpIHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQgPSBbXTtcbiAgICAgICAgfVxuICAgICAgICBlbHNlIGlmIChsb2dTZXR0aW5ncy5wcm9wZXJ0aWVzVG9JbnN0cnVtZW50Lmxlbmd0aCA9PT0gMCkge1xuICAgICAgICAgICAgcHJvcGVydGllc1RvSW5zdHJ1bWVudCA9IE9iamVjdC5nZXRQcm9wZXJ0eU5hbWVzKG9iamVjdCk7XG4gICAgICAgIH1cbiAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICBwcm9wZXJ0aWVzVG9JbnN0cnVtZW50ID0gbG9nU2V0dGluZ3MucHJvcGVydGllc1RvSW5zdHJ1bWVudDtcbiAgICAgICAgfVxuICAgICAgICBmb3IgKGNvbnN0IHByb3BlcnR5TmFtZSBvZiBwcm9wZXJ0aWVzVG9JbnN0cnVtZW50KSB7XG4gICAgICAgICAgICBpZiAobG9nU2V0dGluZ3MuZXhjbHVkZWRQcm9wZXJ0aWVzLmluY2x1ZGVzKHByb3BlcnR5TmFtZSkpIHtcbiAgICAgICAgICAgICAgICBjb250aW51ZTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIC8vIElmIGByZWN1cnNpdmVgIGZsYWcgc2V0IHdlIHdhbnQgdG8gcmVjdXJzaXZlbHkgaW5zdHJ1bWVudCBhbnlcbiAgICAgICAgICAgIC8vIG9iamVjdCBwcm9wZXJ0aWVzIHRoYXQgYXJlbid0IHRoZSBwcm90b3R5cGUgb2JqZWN0LlxuICAgICAgICAgICAgaWYgKGxvZ1NldHRpbmdzLnJlY3Vyc2l2ZSAmJlxuICAgICAgICAgICAgICAgIGxvZ1NldHRpbmdzLmRlcHRoID4gMCAmJlxuICAgICAgICAgICAgICAgIGlzT2JqZWN0KG9iamVjdCwgcHJvcGVydHlOYW1lKSAmJlxuICAgICAgICAgICAgICAgIHByb3BlcnR5TmFtZSAhPT0gXCJfX3Byb3RvX19cIikge1xuICAgICAgICAgICAgICAgIGNvbnN0IG5ld0luc3RydW1lbnRlZE5hbWUgPSBgJHtpbnN0cnVtZW50ZWROYW1lfS4ke3Byb3BlcnR5TmFtZX1gO1xuICAgICAgICAgICAgICAgIGNvbnN0IG5ld0xvZ1NldHRpbmdzID0geyAuLi5sb2dTZXR0aW5ncyB9O1xuICAgICAgICAgICAgICAgIG5ld0xvZ1NldHRpbmdzLmRlcHRoID0gbG9nU2V0dGluZ3MuZGVwdGggLSAxO1xuICAgICAgICAgICAgICAgIG5ld0xvZ1NldHRpbmdzLnByb3BlcnRpZXNUb0luc3RydW1lbnQgPSBbXTtcbiAgICAgICAgICAgICAgICBpbnN0cnVtZW50T2JqZWN0KG9iamVjdFtwcm9wZXJ0eU5hbWVdLCBuZXdJbnN0cnVtZW50ZWROYW1lLCBuZXdMb2dTZXR0aW5ncyk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICB0cnkge1xuICAgICAgICAgICAgICAgIGluc3RydW1lbnRPYmplY3RQcm9wZXJ0eShvYmplY3QsIGluc3RydW1lbnRlZE5hbWUsIHByb3BlcnR5TmFtZSwgbG9nU2V0dGluZ3MpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgICAgICAgICAgaWYgKGVycm9yIGluc3RhbmNlb2YgVHlwZUVycm9yICYmXG4gICAgICAgICAgICAgICAgICAgIGVycm9yLm1lc3NhZ2UuaW5jbHVkZXMoXCJjYW4ndCByZWRlZmluZSBub24tY29uZmlndXJhYmxlIHByb3BlcnR5XCIpKSB7XG4gICAgICAgICAgICAgICAgICAgIGNvbnNvbGUud2FybihgQ2Fubm90IGluc3RydW1lbnQgbm9uLWNvbmZpZ3VyYWJsZSBwcm9wZXJ0eTogJHtpbnN0cnVtZW50ZWROYW1lfToke3Byb3BlcnR5TmFtZX1gKTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgIGxvZ0Vycm9yVG9Db25zb2xlKGVycm9yLCB7IGluc3RydW1lbnRlZE5hbWUsIHByb3BlcnR5TmFtZSB9KTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgZm9yIChjb25zdCBwcm9wZXJ0eU5hbWUgb2YgbG9nU2V0dGluZ3Mubm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50KSB7XG4gICAgICAgICAgICBpZiAobG9nU2V0dGluZ3MuZXhjbHVkZWRQcm9wZXJ0aWVzLmluY2x1ZGVzKHByb3BlcnR5TmFtZSkpIHtcbiAgICAgICAgICAgICAgICBjb250aW51ZTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIHRyeSB7XG4gICAgICAgICAgICAgICAgaW5zdHJ1bWVudE9iamVjdFByb3BlcnR5KG9iamVjdCwgaW5zdHJ1bWVudGVkTmFtZSwgcHJvcGVydHlOYW1lLCBsb2dTZXR0aW5ncyk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBjYXRjaCAoZXJyb3IpIHtcbiAgICAgICAgICAgICAgICBsb2dFcnJvclRvQ29uc29sZShlcnJvciwgeyBpbnN0cnVtZW50ZWROYW1lLCBwcm9wZXJ0eU5hbWUgfSk7XG4gICAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICB9XG4gICAgY29uc3Qgc2VuZEZhY3RvcnkgPSBmdW5jdGlvbiAoZXZlbnRJZCwgJHNlbmRNZXNzYWdlc1RvTG9nZ2VyKSB7XG4gICAgICAgIGxldCBtZXNzYWdlcyA9IFtdO1xuICAgICAgICAvLyBkZWJvdW5jZSBzZW5kaW5nIHF1ZXVlZCBtZXNzYWdlc1xuICAgICAgICBjb25zdCBfc2VuZCA9IGRlYm91bmNlKGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICRzZW5kTWVzc2FnZXNUb0xvZ2dlcihldmVudElkLCBtZXNzYWdlcyk7XG4gICAgICAgICAgICAvLyBjbGVhciB0aGUgcXVldWVcbiAgICAgICAgICAgIG1lc3NhZ2VzID0gW107XG4gICAgICAgIH0sIDEwMCk7XG4gICAgICAgIHJldHVybiBmdW5jdGlvbiAobXNnVHlwZSwgbXNnKSB7XG4gICAgICAgICAgICAvLyBxdWV1ZSB0aGUgbWVzc2FnZVxuICAgICAgICAgICAgbWVzc2FnZXMucHVzaCh7IHR5cGU6IG1zZ1R5cGUsIGNvbnRlbnQ6IG1zZyB9KTtcbiAgICAgICAgICAgIF9zZW5kKCk7XG4gICAgICAgIH07XG4gICAgfTtcbiAgICBjb25zdCBzZW5kID0gc2VuZEZhY3RvcnkoZXZlbnRJZCwgc2VuZE1lc3NhZ2VzVG9Mb2dnZXIpO1xuICAgIGZ1bmN0aW9uIGluc3RydW1lbnRKUyhKU0luc3RydW1lbnRSZXF1ZXN0cykge1xuICAgICAgICAvLyBUaGUgSlMgSW5zdHJ1bWVudCBSZXF1ZXN0cyBhcmUgc2V0dXAgYW5kIHZhbGlkYXRlZCBweXRob24gc2lkZVxuICAgICAgICAvLyBpbmNsdWRpbmcgc2V0dGluZyBkZWZhdWx0cyBmb3IgbG9nU2V0dGluZ3MuXG4gICAgICAgIC8vIE1vcmUgZGV0YWlscyBhYm91dCBob3cgdGhpcyBmdW5jdGlvbiBpcyBpbnZva2VkIGFyZSBpblxuICAgICAgICAvLyBjb250ZW50L2phdmFzY3JpcHQtaW5zdHJ1bWVudC1jb250ZW50LXNjb3BlLnRzXG4gICAgICAgIEpTSW5zdHJ1bWVudFJlcXVlc3RzLmZvckVhY2goZnVuY3Rpb24gKGl0ZW0pIHtcbiAgICAgICAgICAgIGluc3RydW1lbnRPYmplY3QoZXZhbChpdGVtLm9iamVjdCksIGl0ZW0uaW5zdHJ1bWVudGVkTmFtZSwgaXRlbS5sb2dTZXR0aW5ncyk7XG4gICAgICAgIH0pO1xuICAgIH1cbiAgICAvLyBUaGlzIHdob2xlIGZ1bmN0aW9uIGdldEluc3RydW1lbnRKUyByZXR1cm5zIGp1c3QgdGhlIGZ1bmN0aW9uIGBpbnN0cnVtZW50SlNgLlxuICAgIHJldHVybiBpbnN0cnVtZW50SlM7XG59XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2lhbk10YVc1emRISjFiV1Z1ZEhNdWFuTWlMQ0p6YjNWeVkyVlNiMjkwSWpvaUlpd2ljMjkxY21ObGN5STZXeUl1TGk4dUxpOHVMaTl6Y21NdmJHbGlMMnB6TFdsdWMzUnlkVzFsYm5SekxuUnpJbDBzSW01aGJXVnpJanBiWFN3aWJXRndjR2x1WjNNaU9pSkJRVUZCTEdsRlFVRnBSVHRCUVVOcVJTeHZSa0ZCYjBZN1FVRTRRbkJHTEUxQlFVMHNWVUZCVlN4bFFVRmxMRU5CUVVNc1QwRkJaU3hGUVVGRkxHOUNRVUZ2UWp0SlFVTnVSVHM3TzA5QlIwYzdTVUZGU0N4M1EwRkJkME03U1VGRGVFTXNiME5CUVc5RE8wbEJRM0JETEhGRFFVRnhRenRKUVVOeVF5eHBRMEZCYVVNN1NVRkRha01zYVVOQlFXbERPMGxCUTJwRExIRkNRVUZ4UWp0SlFVTnlRaXhsUVVGbE8wbEJRMllzVFVGQlRUdEpRVVZPTEcxRlFVRnRSVHRKUVVOdVJTeE5RVUZOTEZkQlFWY3NSMEZCUnl4SFFVRkhMRU5CUVVNN1NVRkRlRUlzWVVGQllUdEpRVU5pTEUxQlFVMHNWVUZCVlN4SFFVRkhMRWxCUVVrc1RVRkJUU3hGUVVGRkxFTkJRVU03U1VGRGFFTXNLME5CUVN0RE8wbEJReTlETEVsQlFVa3NTMEZCU3l4SFFVRkhMRXRCUVVzc1EwRkJRenRKUVVOc1FpeG5SRUZCWjBRN1NVRkRhRVFzU1VGQlNTeFBRVUZQTEVkQlFVY3NRMEZCUXl4RFFVRkRPMGxCUldoQ0xEQkNRVUV3UWp0SlFVTXhRaXhOUVVGTkxGZEJRVmNzUjBGQlJ6dFJRVU5zUWl4SlFVRkpMRVZCUVVVc1RVRkJUVHRSUVVOYUxFZEJRVWNzUlVGQlJTeExRVUZMTzFGQlExWXNWVUZCVlN4RlFVRkZMR0ZCUVdFN1VVRkRla0lzV1VGQldTeEZRVUZGTEdWQlFXVTdVVUZETjBJc1IwRkJSeXhGUVVGRkxFdEJRVXM3VVVGRFZpeFZRVUZWTEVWQlFVVXNZVUZCWVR0UlFVTjZRaXhoUVVGaExFVkJRVVVzWjBKQlFXZENPMHRCUTJoRExFTkJRVU03U1VGRlJpeHZSa0ZCYjBZN1NVRkRjRVlzZVVWQlFYbEZPMGxCUTNwRkxFMUJRVTBzUTBGQlF5eHhRa0ZCY1VJc1IwRkJSeXhWUVVGVkxFOUJRVThzUlVGQlJTeEpRVUZKTzFGQlEzQkVMRWxCUVVrc1QwRkJUeXhMUVVGTExGTkJRVk1zUlVGQlJUdFpRVU42UWl4TlFVRk5MRWxCUVVrc1MwRkJTeXhEUVVGRExEWkRRVUUyUXl4RFFVRkRMRU5CUVVNN1UwRkRhRVU3VVVGRFJDeEpRVUZKTEVWQlFVVXNSMEZCUnl4TlFVRk5MRU5CUVVNc2QwSkJRWGRDTEVOQlFVTXNUMEZCVHl4RlFVRkZMRWxCUVVrc1EwRkJReXhEUVVGRE8xRkJRM2hFTEVsQlFVa3NTMEZCU3l4SFFVRkhMRTFCUVUwc1EwRkJReXhqUVVGakxFTkJRVU1zVDBGQlR5eERRVUZETEVOQlFVTTdVVUZETTBNc1QwRkJUeXhGUVVGRkxFdEJRVXNzVTBGQlV5eEpRVUZKTEV0QlFVc3NTMEZCU3l4SlFVRkpMRVZCUVVVN1dVRkRla01zUlVGQlJTeEhRVUZITEUxQlFVMHNRMEZCUXl4M1FrRkJkMElzUTBGQlF5eExRVUZMTEVWQlFVVXNTVUZCU1N4RFFVRkRMRU5CUVVNN1dVRkRiRVFzUzBGQlN5eEhRVUZITEUxQlFVMHNRMEZCUXl4alFVRmpMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU03VTBGRGRFTTdVVUZEUkN4UFFVRlBMRVZCUVVVc1EwRkJRenRKUVVOYUxFTkJRVU1zUTBGQlF6dEpRVVZHTEUxQlFVMHNRMEZCUXl4blFrRkJaMElzUjBGQlJ5eFZRVUZWTEU5QlFVODdVVUZEZWtNc1NVRkJTU3hQUVVGUExFdEJRVXNzVTBGQlV5eEZRVUZGTzFsQlEzcENMRTFCUVUwc1NVRkJTU3hMUVVGTExFTkJRVU1zZDBOQlFYZERMRU5CUVVNc1EwRkJRenRUUVVNelJEdFJRVU5FTEVsQlFVa3NTMEZCU3l4SFFVRkhMRTFCUVUwc1EwRkJReXh0UWtGQmJVSXNRMEZCUXl4UFFVRlBMRU5CUVVNc1EwRkJRenRSUVVOb1JDeEpRVUZKTEV0QlFVc3NSMEZCUnl4TlFVRk5MRU5CUVVNc1kwRkJZeXhEUVVGRExFOUJRVThzUTBGQlF5eERRVUZETzFGQlF6TkRMRTlCUVU4c1MwRkJTeXhMUVVGTExFbEJRVWtzUlVGQlJUdFpRVU55UWl4TFFVRkxMRWRCUVVjc1MwRkJTeXhEUVVGRExFMUJRVTBzUTBGQlF5eE5RVUZOTEVOQlFVTXNiVUpCUVcxQ0xFTkJRVU1zUzBGQlN5eERRVUZETEVOQlFVTXNRMEZCUXp0WlFVTjRSQ3hMUVVGTExFZEJRVWNzVFVGQlRTeERRVUZETEdOQlFXTXNRMEZCUXl4TFFVRkxMRU5CUVVNc1EwRkJRenRUUVVOMFF6dFJRVU5FTEc5RVFVRnZSRHRSUVVOd1JDeFBRVUZQTEV0QlFVc3NRMEZCUXp0SlFVTm1MRU5CUVVNc1EwRkJRenRKUVVWR0xHOURRVUZ2UXp0SlFVTndReXhUUVVGVExGRkJRVkVzUTBGQlF5eEpRVUZKTEVWQlFVVXNTVUZCU1N4RlFVRkZMRmxCUVhGQ0xFdEJRVXM3VVVGRGRFUXNTVUZCU1N4UFFVRlBMRU5CUVVNN1VVRkRXaXhKUVVGSkxFbEJRVWtzUTBGQlF6dFJRVU5VTEVsQlFVa3NUMEZCVHl4RFFVRkRPMUZCUTFvc1NVRkJTU3hUUVVGVExFTkJRVU03VVVGRFpDeEpRVUZKTEUxQlFVMHNRMEZCUXp0UlFVVllMRTFCUVUwc1MwRkJTeXhIUVVGSE8xbEJRMW9zVFVGQlRTeEpRVUZKTEVkQlFVY3NTVUZCU1N4RFFVRkRMRWRCUVVjc1JVRkJSU3hIUVVGSExGTkJRVk1zUTBGQlF6dFpRVU53UXl4SlFVRkpMRWxCUVVrc1IwRkJSeXhKUVVGSkxFVkJRVVU3WjBKQlEyWXNUMEZCVHl4SFFVRkhMRlZCUVZVc1EwRkJReXhMUVVGTExFVkJRVVVzU1VGQlNTeEhRVUZITEVsQlFVa3NRMEZCUXl4RFFVRkRPMkZCUXpGRE8ybENRVUZOTzJkQ1FVTk1MRTlCUVU4c1IwRkJSeXhKUVVGSkxFTkJRVU03WjBKQlEyWXNTVUZCU1N4RFFVRkRMRk5CUVZNc1JVRkJSVHR2UWtGRFpDeE5RVUZOTEVkQlFVY3NTVUZCU1N4RFFVRkRMRXRCUVVzc1EwRkJReXhQUVVGUExFVkJRVVVzU1VGQlNTeERRVUZETEVOQlFVTTdiMEpCUTI1RExFOUJRVThzUjBGQlJ5eEpRVUZKTEVkQlFVY3NTVUZCU1N4RFFVRkRPMmxDUVVOMlFqdGhRVU5HTzFGQlEwZ3NRMEZCUXl4RFFVRkRPMUZCUlVZc1QwRkJUenRaUVVOTUxFOUJRVThzUjBGQlJ5eEpRVUZKTEVOQlFVTTdXVUZEWml4SlFVRkpMRWRCUVVjc1UwRkJVeXhEUVVGRE8xbEJRMnBDTEZOQlFWTXNSMEZCUnl4SlFVRkpMRU5CUVVNc1IwRkJSeXhGUVVGRkxFTkJRVU03V1VGRGRrSXNUVUZCVFN4UFFVRlBMRWRCUVVjc1UwRkJVeXhKUVVGSkxFTkJRVU1zVDBGQlR5eERRVUZETzFsQlEzUkRMRWxCUVVrc1EwRkJReXhQUVVGUExFVkJRVVU3WjBKQlExb3NUMEZCVHl4SFFVRkhMRlZCUVZVc1EwRkJReXhMUVVGTExFVkJRVVVzU1VGQlNTeERRVUZETEVOQlFVTTdZVUZEYmtNN1dVRkRSQ3hKUVVGSkxFOUJRVThzUlVGQlJUdG5Ra0ZEV0N4TlFVRk5MRWRCUVVjc1NVRkJTU3hEUVVGRExFdEJRVXNzUTBGQlF5eFBRVUZQTEVWQlFVVXNTVUZCU1N4RFFVRkRMRU5CUVVNN1owSkJRMjVETEU5QlFVOHNSMEZCUnl4SlFVRkpMRWRCUVVjc1NVRkJTU3hEUVVGRE8yRkJRM1pDTzFsQlJVUXNUMEZCVHl4TlFVRk5MRU5CUVVNN1VVRkRhRUlzUTBGQlF5eERRVUZETzBsQlEwb3NRMEZCUXp0SlFVVkVMRGhEUVVFNFF6dEpRVU01UXl4VFFVRlRMRzFDUVVGdFFpeERRVUZETEU5QlFWa3NSVUZCUlN4cFFrRkJNRUlzUzBGQlN6dFJRVU40UlN4SlFVRkpMRTlCUVU4c1MwRkJTeXhSUVVGUkxFTkJRVU1zU1VGQlNTeEZRVUZGTzFsQlF6ZENMRTlCUVU4c1QwRkJUeXhEUVVGRExFOUJRVThzUTBGQlF6dFRRVU40UWp0UlFVTkVMRWxCUVVrc1QwRkJUeXhEUVVGRExGVkJRVlVzUzBGQlN5eEpRVUZKTEVWQlFVVTdXVUZETDBJc1QwRkJUeXhQUVVGUExFZEJRVWNzVDBGQlR5eERRVUZETEU5QlFVOHNRMEZCUXp0VFFVTnNRenRSUVVWRUxFbEJRVWtzV1VGQldTeEhRVUZITEVOQlFVTXNRMEZCUXp0UlFVTnlRaXhOUVVGTkxGRkJRVkVzUjBGQlJ5eFBRVUZQTEVOQlFVTXNWVUZCVlN4RFFVRkRMRlZCUVZVc1EwRkJRenRSUVVNdlF5eExRVUZMTEVsQlFVa3NRMEZCUXl4SFFVRkhMRU5CUVVNc1JVRkJSU3hEUVVGRExFZEJRVWNzVVVGQlVTeERRVUZETEUxQlFVMHNSVUZCUlN4RFFVRkRMRVZCUVVVc1JVRkJSVHRaUVVONFF5eE5RVUZOTEU5QlFVOHNSMEZCUnl4UlFVRlJMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU03V1VGRE5VSXNTVUZCU1N4UFFVRlBMRXRCUVVzc1QwRkJUeXhGUVVGRk8yZENRVU4yUWl4SlFVRkpMRWxCUVVrc1IwRkJSeXh0UWtGQmJVSXNRMEZCUXl4UFFVRlBMRU5CUVVNc1ZVRkJWU3hGUVVGRkxHTkJRV01zUTBGQlF5eERRVUZETzJkQ1FVTnVSU3hKUVVGSkxFbEJRVWtzUjBGQlJ5eEhRVUZITEU5QlFVOHNRMEZCUXl4UFFVRlBMRWRCUVVjc1IwRkJSeXhIUVVGSExGbEJRVmtzUTBGQlF6dG5Ra0ZEYmtRc1NVRkJTU3hKUVVGSkxFZEJRVWNzUjBGQlJ5eFBRVUZQTEVOQlFVTXNSVUZCUlN4RFFVRkRPMmRDUVVONlFpeEpRVUZKTEVsQlFVa3NSMEZCUnl4SFFVRkhMRTlCUVU4c1EwRkJReXhUUVVGVExFTkJRVU03WjBKQlEyaERMRWxCUVVrc1kwRkJZeXhGUVVGRk8yOUNRVU5zUWl4SlFVRkpMRWxCUVVrc1IwRkJSeXhIUVVGSExFOUJRVThzUTBGQlF5eE5RVUZOTEVOQlFVTTdiMEpCUXpkQ0xFbEJRVWtzU1VGQlNTeEhRVUZITEVkQlFVY3NUMEZCVHl4RFFVRkRMRXRCUVVzc1EwRkJReXhQUVVGUExFTkJRVU03YjBKQlEzQkRMRWxCUVVrc1NVRkJTU3hIUVVGSExFZEJRVWNzVDBGQlR5eERRVUZETEV0QlFVc3NRMEZCUXl4VlFVRlZMRU5CUVVNN2FVSkJRM2hETzJkQ1FVTkVMRWxCUVVrc1QwRkJUeXhEUVVGRExFOUJRVThzUzBGQlN5eEhRVUZITEVWQlFVVTdiMEpCUXpOQ0xFbEJRVWtzU1VGQlNTeEhRVUZITEVkQlFVY3NUMEZCVHl4RFFVRkRMRWxCUVVrc1EwRkJRenRwUWtGRE5VSTdaMEpCUTBRc1NVRkJTU3hKUVVGSkxFZEJRVWNzUTBGQlF6dG5Ra0ZEV2l4UFFVRlBMRWxCUVVrc1EwRkJRenRoUVVOaU8xbEJRMFFzU1VGQlNTeFBRVUZQTEVOQlFVTXNVVUZCVVN4TFFVRkxMRU5CUVVNc1NVRkJTU3hQUVVGUExFTkJRVU1zVDBGQlR5eExRVUZMTEU5QlFVOHNRMEZCUXl4UFFVRlBMRVZCUVVVN1owSkJRMnBGTEZsQlFWa3NSVUZCUlN4RFFVRkRPMkZCUTJoQ08xTkJRMFk3U1VGRFNDeERRVUZETzBsQlJVUXNaME5CUVdkRE8wbEJRMmhETEZOQlFWTXNaVUZCWlN4RFFVTjBRaXhOUVVGTkxFVkJRMDRzY1VKQlFUaENMRXRCUVVzN1VVRkZia01zTkVKQlFUUkNPMUZCUXpWQ0xFbEJRVWs3V1VGRFJpeEpRVUZKTEUxQlFVMHNTMEZCU3l4SlFVRkpMRVZCUVVVN1owSkJRMjVDTEU5QlFVOHNUVUZCVFN4RFFVRkRPMkZCUTJZN1dVRkRSQ3hKUVVGSkxFOUJRVThzVFVGQlRTeExRVUZMTEZWQlFWVXNSVUZCUlR0blFrRkRhRU1zVDBGQlR5eHJRa0ZCYTBJc1EwRkJReXhEUVVGRExFTkJRVU1zVFVGQlRTeERRVUZETEZGQlFWRXNSVUZCUlN4RFFVRkRMRU5CUVVNc1EwRkJReXhWUVVGVkxFTkJRVU03WVVGRE5VUTdXVUZEUkN4SlFVRkpMRTlCUVU4c1RVRkJUU3hMUVVGTExGRkJRVkVzUlVGQlJUdG5Ra0ZET1VJc1QwRkJUeXhOUVVGTkxFTkJRVU03WVVGRFpqdFpRVU5FTEUxQlFVMHNWMEZCVnl4SFFVRkhMRVZCUVVVc1EwRkJRenRaUVVOMlFpeFBRVUZQTEVsQlFVa3NRMEZCUXl4VFFVRlRMRU5CUVVNc1RVRkJUU3hGUVVGRkxGVkJRVlVzUjBGQlJ5eEZRVUZGTEV0QlFVczdaMEpCUTJoRUxFbEJRVWtzUzBGQlN5eExRVUZMTEVsQlFVa3NSVUZCUlR0dlFrRkRiRUlzVDBGQlR5eE5RVUZOTEVOQlFVTTdhVUpCUTJZN1owSkJRMFFzU1VGQlNTeFBRVUZQTEV0QlFVc3NTMEZCU3l4VlFVRlZMRVZCUVVVN2IwSkJReTlDTEU5QlFVOHNhMEpCUVd0Q0xFTkJRVU1zUTBGQlF5eERRVUZETEV0QlFVc3NRMEZCUXl4UlFVRlJMRVZCUVVVc1EwRkJReXhEUVVGRExFTkJRVU1zVlVGQlZTeERRVUZETzJsQ1FVTXpSRHRuUWtGRFJDeEpRVUZKTEU5QlFVOHNTMEZCU3l4TFFVRkxMRkZCUVZFc1JVRkJSVHR2UWtGRE4wSXNjVU5CUVhGRE8yOUNRVU55UXl4SlFVRkpMR2xDUVVGcFFpeEpRVUZKTEV0QlFVc3NSVUZCUlR0M1FrRkRPVUlzUzBGQlN5eEhRVUZITEV0QlFVc3NRMEZCUXl4bFFVRmxMRU5CUVVNN2NVSkJReTlDTzI5Q1FVVkVMSGxDUVVGNVFqdHZRa0ZEZWtJc1NVRkJTU3hMUVVGTExGbEJRVmtzVjBGQlZ5eEZRVUZGTzNkQ1FVTm9ReXhQUVVGUExHMUNRVUZ0UWl4RFFVRkRMRXRCUVVzc1EwRkJReXhEUVVGRE8zRkNRVU51UXp0dlFrRkZSQ3dyUWtGQkswSTdiMEpCUXk5Q0xFbEJRVWtzUjBGQlJ5eExRVUZMTEVWQlFVVXNTVUZCU1N4WFFVRlhMRU5CUVVNc1QwRkJUeXhEUVVGRExFdEJRVXNzUTBGQlF5eEhRVUZITEVOQlFVTXNSVUZCUlR0M1FrRkRhRVFzVjBGQlZ5eERRVUZETEVsQlFVa3NRMEZCUXl4TFFVRkxMRU5CUVVNc1EwRkJRenQzUWtGRGVFSXNUMEZCVHl4TFFVRkxMRU5CUVVNN2NVSkJRMlE3ZVVKQlFVMDdkMEpCUTB3c1QwRkJUeXhQUVVGUExFdEJRVXNzUTBGQlF6dHhRa0ZEY2tJN2FVSkJRMFk3WjBKQlEwUXNUMEZCVHl4TFFVRkxMRU5CUVVNN1dVRkRaaXhEUVVGRExFTkJRVU1zUTBGQlF6dFRRVU5LTzFGQlFVTXNUMEZCVHl4TFFVRkxMRVZCUVVVN1dVRkRaQ3hQUVVGUExFTkJRVU1zUjBGQlJ5eERRVUZETEdkRFFVRm5ReXhIUVVGSExFdEJRVXNzUTBGQlF5eERRVUZETzFsQlEzUkVMRTlCUVU4c2RVSkJRWFZDTEVkQlFVY3NTMEZCU3l4RFFVRkRPMU5CUTNoRE8wbEJRMGdzUTBGQlF6dEpRVVZFTEZOQlFWTXNNa0pCUVRKQ0xFTkJRVU1zVTBGQlV5eEZRVUZGTEUxQlFVMDdVVUZEY0VRc1RVRkJUU3hIUVVGSExFZEJRVWNzVTBGQlV5eEhRVUZITEVkQlFVY3NSMEZCUnl4TlFVRk5MRU5CUVVNN1VVRkRja01zU1VGQlNTeEhRVUZITEVsQlFVa3NWVUZCVlN4SlFVRkpMRlZCUVZVc1EwRkJReXhIUVVGSExFTkJRVU1zU1VGQlNTeFhRVUZYTEVWQlFVVTdXVUZEZGtRc1QwRkJUeXhKUVVGSkxFTkJRVU03VTBGRFlqdGhRVUZOTEVsQlFVa3NRMEZCUXl4RFFVRkRMRWRCUVVjc1NVRkJTU3hWUVVGVkxFTkJRVU1zUlVGQlJUdFpRVU12UWl4VlFVRlZMRU5CUVVNc1IwRkJSeXhEUVVGRExFZEJRVWNzUTBGQlF5eERRVUZETzFOQlEzSkNPMkZCUVUwN1dVRkRUQ3hWUVVGVkxFTkJRVU1zUjBGQlJ5eERRVUZETEVsQlFVa3NRMEZCUXl4RFFVRkRPMU5CUTNSQ08xRkJRMFFzVDBGQlR5eExRVUZMTEVOQlFVTTdTVUZEWml4RFFVRkRPMGxCUlVRc2VVTkJRWGxETzBsQlEzcERMRk5CUVZNc1VVRkJVU3hEUVVObUxIZENRVUZuUXl4RlFVTm9ReXhMUVVGVkxFVkJRMVlzVlVGQlpTeEZRVU5tTEZOQlFXbENMRVZCUVVVc2FVTkJRV2xETzBsQlEzQkVMRmRCUVdkQ0xFVkJRMmhDTEZkQlFYZENPMUZCUlhoQ0xFbEJRVWtzUzBGQlN5eEZRVUZGTzFsQlExUXNUMEZCVHp0VFFVTlNPMUZCUTBRc1MwRkJTeXhIUVVGSExFbEJRVWtzUTBGQlF6dFJRVVZpTEUxQlFVMHNVMEZCVXl4SFFVRkhMREpDUVVFeVFpeERRVU16UXl4WFFVRlhMRU5CUVVNc1UwRkJVeXhGUVVOeVFpeDNRa0ZCZDBJc1EwRkRla0lzUTBGQlF6dFJRVU5HTEVsQlFVa3NVMEZCVXl4RlFVRkZPMWxCUTJJc1MwRkJTeXhIUVVGSExFdEJRVXNzUTBGQlF6dFpRVU5rTEU5QlFVODdVMEZEVWp0UlFVVkVMR2xHUVVGcFJqdFJRVU5xUml4M1EwRkJkME03VVVGRGVFTXNTVUZCU1N4M1FrRkJkMElzUTBGQlF5eFJRVUZSTEVOQlFVTXNZVUZCWVN4RFFVRkRMRVZCUVVVN1dVRkRjRVFzU1VGQlNTeFhRVUZYTEVkQlFVY3NTVUZCU1N4SFFVRkhMRU5CUVVNc1MwRkJTeXhGUVVGRkxFMUJRVTBzUTBGQlF5eFJRVUZSTEVOQlFVTXNTVUZCU1N4RFFVRkRMRU5CUVVNN1dVRkRka1FzUzBGQlN5eEhRVUZITEZkQlFWY3NRMEZCUXl4SlFVRkpMRU5CUVVNN1dVRkRla0lzU1VGQlNTeFRRVUZUTEVkQlFVY3NUVUZCVFN4RFFVRkRMRTFCUVUwc1EwRkJReXhGUVVGRkxFVkJRVVVzUzBGQlN5eERRVUZETEVsQlFVa3NRMEZCUXl4VlFVRlZMRVZCUVVVc1EwRkJReXhGUVVGRExFbEJRVWtzUlVGQlJTeExRVUZMTEVWQlFVTXNSVUZCUlN4RlFVRkZMRU5CUVVNc1EwRkJReXhGUVVGRExFTkJRVU1zU1VGQlNTeERRVUZETEVWQlFVVXNTMEZCU3l4RlFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU03V1VGRGFFY3NWVUZCVlN4SFFVRkhMRk5CUVZNc1EwRkJRenRUUVVONFFqdFJRVWRFTEUxQlFVMHNSMEZCUnl4SFFVRkhPMWxCUTFZc1UwRkJVenRaUVVOVUxFMUJRVTBzUlVGQlJTeDNRa0ZCZDBJN1dVRkRhRU1zUzBGQlN5eEZRVUZGTEdWQlFXVXNRMEZCUXl4TFFVRkxMRVZCUVVVc1YwRkJWeXhEUVVGRExIRkNRVUZ4UWl4RFFVRkRPMWxCUTJoRkxGVkJRVlVzUlVGQlJTeGxRVUZsTEVOQlFVTXNWVUZCVlN4RFFVRkRPMWxCUTNaRExGTkJRVk1zUlVGQlJTeFhRVUZYTEVOQlFVTXNVMEZCVXp0WlFVTm9ReXhWUVVGVkxFVkJRVVVzVjBGQlZ5eERRVUZETEZWQlFWVTdXVUZEYkVNc1UwRkJVeXhGUVVGRkxGZEJRVmNzUTBGQlF5eFRRVUZUTzFsQlEyaERMRkZCUVZFc1JVRkJSU3hYUVVGWExFTkJRVU1zVVVGQlVUdFpRVU01UWl4aFFVRmhMRVZCUVVVc1YwRkJWeXhEUVVGRExHRkJRV0U3V1VGRGVFTXNVMEZCVXl4RlFVRkZMRmRCUVZjc1EwRkJReXhUUVVGVE8xbEJRMmhETEU5QlFVOHNSVUZCUlN4UFFVRlBMRVZCUVVVN1UwRkRia0lzUTBGQlF6dFJRVVZHTEVsQlFVazdXVUZEUml4SlFVRkpMRU5CUVVNc1ZVRkJWU3hGUVVGRkxFZEJRVWNzUTBGQlF5eERRVUZETzFOQlEzWkNPMUZCUVVNc1QwRkJUeXhMUVVGTExFVkJRVVU3V1VGRFpDeFBRVUZQTEVOQlFVTXNSMEZCUnl4RFFVRkRMR3REUVVGclF5eERRVUZETEVOQlFVTTdXVUZEYUVRc2FVSkJRV2xDTEVOQlFVTXNTMEZCU3l4RFFVRkRMRU5CUVVNN1UwRkRNVUk3VVVGRlJDeExRVUZMTEVkQlFVY3NTMEZCU3l4RFFVRkRPMGxCUTJoQ0xFTkJRVU03U1VGRlJDeG5Ra0ZCWjBJN1NVRkRhRUlzVTBGQlV5eFBRVUZQTEVOQlEyUXNkMEpCUVdkRExFVkJRMmhETEVsQlFXZENMRVZCUTJoQ0xGVkJRV1VzUlVGRFppeFhRVUZuUWl4RlFVTm9RaXhYUVVGM1FqdFJRVVY0UWl4SlFVRkpMRXRCUVVzc1JVRkJSVHRaUVVOVUxFOUJRVTg3VTBGRFVqdFJRVU5FTEV0QlFVc3NSMEZCUnl4SlFVRkpMRU5CUVVNN1VVRkZZaXhOUVVGTkxGTkJRVk1zUjBGQlJ5d3lRa0ZCTWtJc1EwRkRNME1zVjBGQlZ5eERRVUZETEZOQlFWTXNSVUZEY2tJc2QwSkJRWGRDTEVOQlEzcENMRU5CUVVNN1VVRkRSaXhKUVVGSkxGTkJRVk1zUlVGQlJUdFpRVU5pTEV0QlFVc3NSMEZCUnl4TFFVRkxMRU5CUVVNN1dVRkRaQ3hQUVVGUE8xTkJRMUk3VVVGRlJDeEpRVUZKTzFsQlEwWXNjVVZCUVhGRk8xbEJRM0pGTEUxQlFVMHNWVUZCVlN4SFFVRmhMRVZCUVVVc1EwRkJRenRaUVVOb1F5eExRVUZMTEUxQlFVMHNSMEZCUnl4SlFVRkpMRWxCUVVrc1JVRkJSVHRuUWtGRGRFSXNWVUZCVlN4RFFVRkRMRWxCUVVrc1EwRkRZaXhsUVVGbExFTkJRVU1zUjBGQlJ5eEZRVUZGTEZkQlFWY3NRMEZCUXl4eFFrRkJjVUlzUTBGQlF5eERRVU40UkN4RFFVRkRPMkZCUTBnN1dVRkZSQ3hKUVVGSkxIZENRVUYzUWl4TFFVRkxMQ3RDUVVFclFpeEZRVUZGTzJkQ1FVTTVSQ3hKUVVGSkxGTkJRVk1zUjBGQlJ5eE5RVUZOTEVOQlFVTXNUVUZCVFN4RFFVRkRMRVZCUVVVc1JVRkJSU3hMUVVGTExFTkJRVU1zU1VGQlNTeERRVUZETEZWQlFWVXNSVUZCUlN4RFFVRkRMRVZCUVVNc1NVRkJTU3hGUVVGRkxFdEJRVXNzUlVGQlF5eEZRVUZGTEVWQlFVVXNRMEZCUXl4RFFVRkRMRVZCUVVNc1EwRkJReXhKUVVGSkxFTkJRVU1zUlVGQlJTeExRVUZMTEVWQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJRenRuUWtGRGFFY3NWVUZCVlN4SFFVRkhMRk5CUVZNc1EwRkJRenRoUVVNeFFqdFpRVVZFTEUxQlFVMHNSMEZCUnl4SFFVRkhPMmRDUVVOV0xGTkJRVk1zUlVGQlJTeFhRVUZYTEVOQlFVTXNTVUZCU1R0blFrRkRNMElzVFVGQlRTeEZRVUZGTEhkQ1FVRjNRanRuUWtGRGFFTXNTVUZCU1N4RlFVRkZMRlZCUVZVN1owSkJRMmhDTEV0QlFVc3NSVUZCUlN4RlFVRkZPMmRDUVVOVUxGVkJRVlVzUlVGQlJTeGxRVUZsTEVOQlFVTXNWVUZCVlN4RFFVRkRPMmRDUVVOMlF5eFRRVUZUTEVWQlFVVXNWMEZCVnl4RFFVRkRMRk5CUVZNN1owSkJRMmhETEZWQlFWVXNSVUZCUlN4WFFVRlhMRU5CUVVNc1ZVRkJWVHRuUWtGRGJFTXNVMEZCVXl4RlFVRkZMRmRCUVZjc1EwRkJReXhUUVVGVE8yZENRVU5vUXl4UlFVRlJMRVZCUVVVc1YwRkJWeXhEUVVGRExGRkJRVkU3WjBKQlF6bENMR0ZCUVdFc1JVRkJSU3hYUVVGWExFTkJRVU1zWVVGQllUdG5Ra0ZEZUVNc1UwRkJVeXhGUVVGRkxGZEJRVmNzUTBGQlF5eFRRVUZUTzJkQ1FVTm9ReXhQUVVGUExFVkJRVVVzVDBGQlR5eEZRVUZGTzJGQlEyNUNMRU5CUVVNN1dVRkRSaXhKUVVGSkxFTkJRVU1zVTBGQlV5eEZRVUZGTEVkQlFVY3NRMEZCUXl4RFFVRkRPMU5CUTNSQ08xRkJRVU1zVDBGQlR5eExRVUZMTEVWQlFVVTdXVUZEWkN4UFFVRlBMRU5CUVVNc1IwRkJSeXhEUVVOVUxHdERRVUZyUXl4SFFVRkhMSGRDUVVGM1FpeERRVU01UkN4RFFVRkRPMWxCUTBZc2FVSkJRV2xDTEVOQlFVTXNTMEZCU3l4RFFVRkRMRU5CUVVNN1UwRkRNVUk3VVVGRFJDeExRVUZMTEVkQlFVY3NTMEZCU3l4RFFVRkRPMGxCUTJoQ0xFTkJRVU03U1VGRlJDeFRRVUZUTEdsQ1FVRnBRaXhEUVVGRExFdEJRVXNzUlVGQlJTeFZRVUZsTEV0QlFVczdVVUZEY0VRc1QwRkJUeXhEUVVGRExFdEJRVXNzUTBGQlF5eDFRa0ZCZFVJc1IwRkJSeXhMUVVGTExFTkJRVU1zU1VGQlNTeERRVUZETEVOQlFVTTdVVUZEY0VRc1QwRkJUeXhEUVVGRExFdEJRVXNzUTBGQlF5d3dRa0ZCTUVJc1IwRkJSeXhMUVVGTExFTkJRVU1zVDBGQlR5eERRVUZETEVOQlFVTTdVVUZETVVRc1QwRkJUeXhEUVVGRExFdEJRVXNzUTBGQlF5d3lRa0ZCTWtJc1IwRkJSeXhMUVVGTExFTkJRVU1zVVVGQlVTeERRVUZETEVOQlFVTTdVVUZETlVRc1QwRkJUeXhEUVVGRExFdEJRVXNzUTBGQlF5dzRRa0ZCT0VJc1IwRkJSeXhMUVVGTExFTkJRVU1zVlVGQlZTeERRVUZETEVOQlFVTTdVVUZEYWtVc1QwRkJUeXhEUVVGRExFdEJRVXNzUTBGQlF5eDNRa0ZCZDBJc1IwRkJSeXhMUVVGTExFTkJRVU1zUzBGQlN5eERRVUZETEVOQlFVTTdVVUZEZEVRc1NVRkJTU3hQUVVGUExFVkJRVVU3V1VGRFdDeFBRVUZQTEVOQlFVTXNTMEZCU3l4RFFVRkRMREJDUVVFd1FpeEhRVUZITEVsQlFVa3NRMEZCUXl4VFFVRlRMRU5CUVVNc1QwRkJUeXhEUVVGRExFTkJRVU1zUTBGQlF6dFRRVU55UlR0SlFVTklMRU5CUVVNN1NVRkZSQ3gzUTBGQmQwTTdTVUZEZUVNc1UwRkJVeXhoUVVGaE8xRkJRM0JDTEVsQlFVa3NTMEZCU3l4RFFVRkRPMUZCUlZZc1NVRkJTVHRaUVVOR0xFMUJRVTBzU1VGQlNTeExRVUZMTEVWQlFVVXNRMEZCUXp0VFFVTnVRanRSUVVGRExFOUJRVThzUjBGQlJ5eEZRVUZGTzFsQlExb3NTMEZCU3l4SFFVRkhMRWRCUVVjc1EwRkJReXhMUVVGTExFTkJRVU03VTBGRGJrSTdVVUZGUkN4UFFVRlBMRXRCUVVzc1EwRkJRenRKUVVObUxFTkJRVU03U1VGRlJDd3dRMEZCTUVNN1NVRkRNVU1zVFVGQlRTeE5RVUZOTEVkQlFVY3NWVUZCVlN4TlFVRmpMRVZCUVVVc1IwRkJSeXhGUVVGRkxGRkJRVkU3VVVGRGNFUXNUVUZCVFN4TFFVRkxMRWRCUVVjc1RVRkJUU3hEUVVGRExFdEJRVXNzUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXp0UlFVTm9ReXhQUVVGUExGRkJRVkU3V1VGRFlpeERRVUZETEVOQlFVTXNRMEZCUXl4TFFVRkxMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU1zUlVGQlJTeERRVUZETEZGQlFWRXNRMEZCUXl4RFFVRkRMRWxCUVVrc1EwRkJReXhIUVVGSExFTkJRVU1zUTBGQlF5eERRVUZETEUxQlFVMHNRMEZCUXl4TFFVRkxMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU1zVVVGQlVTeERRVUZETEVOQlFVTTdXVUZEZEVVc1EwRkJReXhEUVVGRExFdEJRVXNzUTBGQlF6dEpRVU5hTEVOQlFVTXNRMEZCUXp0SlFVVkdMRk5CUVZNc01rSkJRVEpDTEVOQlFVTXNXVUZCV1N4SFFVRkhMRXRCUVVzN1VVRkRka1FzVFVGQlRTeExRVUZMTEVkQlFVY3NZVUZCWVN4RlFVRkZMRU5CUVVNc1NVRkJTU3hGUVVGRkxFTkJRVU1zUzBGQlN5eERRVUZETEVsQlFVa3NRMEZCUXl4RFFVRkRPMUZCUTJwRUxHOUVRVUZ2UkR0UlFVTndSQ3hOUVVGTkxHRkJRV0VzUjBGQlJ6dFpRVU53UWl4VFFVRlRMRVZCUVVVc1JVRkJSVHRaUVVOaUxGVkJRVlVzUlVGQlJTeEZRVUZGTzFsQlEyUXNVMEZCVXl4RlFVRkZMRVZCUVVVN1dVRkRZaXhSUVVGUkxFVkJRVVVzUlVGQlJUdFpRVU5hTEdGQlFXRXNSVUZCUlN4RlFVRkZPMWxCUTJwQ0xGTkJRVk1zUlVGQlJTeEZRVUZGTzFOQlEyUXNRMEZCUXp0UlFVTkdMRWxCUVVrc1MwRkJTeXhEUVVGRExFMUJRVTBzUjBGQlJ5eERRVUZETEVWQlFVVTdXVUZEY0VJc1QwRkJUeXhoUVVGaExFTkJRVU03VTBGRGRFSTdVVUZEUkN3d1JVRkJNRVU3VVVGRE1VVXNUVUZCVFN4UlFVRlJMRWRCUVVjc1MwRkJTeXhEUVVGRExFTkJRVU1zUTBGQlF5eERRVUZETzFGQlF6RkNMRWxCUVVrc1EwRkJReXhSUVVGUkxFVkJRVVU3V1VGRFlpeFBRVUZQTEdGQlFXRXNRMEZCUXp0VFFVTjBRanRSUVVORU96czdPenM3T3p0WFFWRkhPMUZCUTBnc1NVRkJTVHRaUVVOR0xFbEJRVWtzVTBGQlV5eEhRVUZITEVWQlFVVXNRMEZCUXp0WlFVTnVRaXhKUVVGSkxHRkJRV0VzUjBGQlJ5eEZRVUZGTEVOQlFVTXNRMEZCUXl3MlFrRkJOa0k3V1VGRGNrUXNUVUZCVFN4aFFVRmhMRWRCUVVjc1VVRkJVU3hEUVVGRExFdEJRVXNzUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXp0WlFVTXhReXhOUVVGTkxGRkJRVkVzUjBGQlJ5eGhRVUZoTEVOQlFVTXNRMEZCUXl4RFFVRkRMRWxCUVVrc1JVRkJSU3hEUVVGRE8xbEJRM2hETEUxQlFVMHNTMEZCU3l4SFFVRkhMRTFCUVUwc1EwRkJReXhoUVVGaExFTkJRVU1zUTBGQlF5eERRVUZETEVWQlFVVXNSMEZCUnl4RlFVRkZMRU5CUVVNc1EwRkJReXhEUVVGRE8xbEJReTlETEUxQlFVMHNVVUZCVVN4SFFVRkhMRXRCUVVzc1EwRkJReXhMUVVGTExFTkJRVU1zVFVGQlRTeEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRPMWxCUTNwRExFMUJRVTBzVFVGQlRTeEhRVUZITEV0QlFVc3NRMEZCUXl4TFFVRkxMRU5CUVVNc1RVRkJUU3hIUVVGSExFTkJRVU1zUTBGQlF5eERRVUZETzFsQlEzWkRMRTFCUVUwc1kwRkJZeXhIUVVGSExFdEJRVXNzUTBGQlF5eExRVUZMTEVOQlFVTXNUVUZCVFN4SFFVRkhMRU5CUVVNc1EwRkJReXhKUVVGSkxFVkJRVVVzUTBGQlF6dFpRVU55UkN4TlFVRk5MRk5CUVZNc1IwRkJSeXhqUVVGakxFTkJRVU1zVDBGQlR5eERRVUZETEZGQlFWRXNRMEZCUXl4RFFVRkRMRU5CUVVNc2VVTkJRWGxETzFsQlF6ZEdMRWxCUVVrc1UwRkJVeXhMUVVGTExFTkJRVU1zUTBGQlF5eEZRVUZGTzJkQ1FVTndRaXhUUVVGVExFZEJRVWNzWTBGQll5eERRVUZETEVOQlFVTXNiMFJCUVc5RU8yRkJRMnBHTzJsQ1FVRk5PMmRDUVVOTUxGTkJRVk1zUjBGQlJ5eGpRVUZqTEVOQlFVTXNTMEZCU3l4RFFVRkRMRU5CUVVNc1JVRkJSU3hUUVVGVExFTkJRVU1zUTBGQlF6dG5Ra0ZETDBNc1lVRkJZU3hIUVVGSExHTkJRV01zUTBGQlF5eExRVUZMTEVOQlEyeERMRk5CUVZNc1IwRkJSeXhEUVVGRExFVkJRMklzWTBGQll5eERRVUZETEUxQlFVMHNRMEZEZEVJc1EwRkJRenRoUVVOSU8xbEJRMFFzVFVGQlRTeFhRVUZYTEVkQlFVYzdaMEpCUTJ4Q0xGTkJRVk03WjBKQlExUXNWVUZCVlN4RlFVRkZMRTFCUVUwN1owSkJRMnhDTEZOQlFWTXNSVUZCUlN4UlFVRlJPMmRDUVVOdVFpeFJRVUZSTzJkQ1FVTlNMR0ZCUVdFN1owSkJRMklzVTBGQlV5eEZRVUZGTEZsQlFWa3NRMEZCUXl4RFFVRkRMRU5CUVVNc1MwRkJTeXhEUVVGRExFdEJRVXNzUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4SlFVRkpMRU5CUVVNc1NVRkJTU3hEUVVGRExFTkJRVU1zU1VGQlNTeEZRVUZGTEVOQlFVTXNRMEZCUXl4RFFVRkRMRVZCUVVVN1lVRkRhRVVzUTBGQlF6dFpRVU5HTEU5QlFVOHNWMEZCVnl4RFFVRkRPMU5CUTNCQ08xRkJRVU1zVDBGQlR5eERRVUZETEVWQlFVVTdXVUZEVml4UFFVRlBMRU5CUVVNc1IwRkJSeXhEUVVOVUxESkRRVUV5UXl4RlFVTXpReXhEUVVGRExFTkJRVU1zVVVGQlVTeEZRVUZGTEVWQlExb3NVVUZCVVN4RFFVTlVMRU5CUVVNN1dVRkRSaXhQUVVGUExHRkJRV0VzUTBGQlF6dFRRVU4wUWp0SlFVTklMRU5CUVVNN1NVRkZSQ3hUUVVGVExGRkJRVkVzUTBGQlF5eE5RVUZOTEVWQlFVVXNXVUZCV1R0UlFVTndReXhKUVVGSkxGRkJRVkVzUTBGQlF6dFJRVU5pTEVsQlFVazdXVUZEUml4UlFVRlJMRWRCUVVjc1RVRkJUU3hEUVVGRExGbEJRVmtzUTBGQlF5eERRVUZETzFOQlEycERPMUZCUVVNc1QwRkJUeXhMUVVGTExFVkJRVVU3V1VGRFpDeFBRVUZQTEV0QlFVc3NRMEZCUXp0VFFVTmtPMUZCUTBRc1NVRkJTU3hSUVVGUkxFdEJRVXNzU1VGQlNTeEZRVUZGTzFsQlEzSkNMSGRDUVVGM1FqdFpRVU40UWl4UFFVRlBMRXRCUVVzc1EwRkJRenRUUVVOa08xRkJRMFFzVDBGQlR5eFBRVUZQTEZGQlFWRXNTMEZCU3l4UlFVRlJMRU5CUVVNN1NVRkRkRU1zUTBGQlF6dEpRVVZFTEdkRFFVRm5RenRKUVVOb1F5eDNSVUZCZDBVN1NVRkRlRVVzZVVWQlFYbEZPMGxCUTNwRkxIZEVRVUYzUkR0SlFVTjRSQ3hUUVVGVExHdENRVUZyUWl4RFFVTjZRaXhWUVVGclFpeEZRVU5zUWl4VlFVRnJRaXhGUVVOc1FpeEpRVUZUTEVWQlExUXNWMEZCZDBJN1VVRkhlRUlzVDBGQlR6dFpRVU5NTEUxQlFVMHNWMEZCVnl4SFFVRkhMREpDUVVFeVFpeERRVUZETEZkQlFWY3NRMEZCUXl4WlFVRlpMRU5CUVVNc1EwRkJRenRaUVVVeFJTeDFRa0ZCZFVJN1dVRkRka0lzU1VGQlNTeFZRVUZWTEVkQlFVY3NSVUZCUlN4RFFVRkRPMWxCUlhCQ0xESkZRVUV5UlR0WlFVTXpSU3hyUWtGQmEwSTdXVUZEYkVJc1NVRkJTU3hWUVVGVkxFbEJRVWtzWlVGQlpTeEZRVUZGTzJkQ1FVTnFReXg1UlVGQmVVVTdaMEpCUTNwRkxHZEZRVUZuUlR0blFrRkRhRVVzYzBOQlFYTkRPMmRDUVVOMFF5dzJRa0ZCTmtJN1owSkJRemRDTEhGRFFVRnhRenRuUWtGRGNrTXNaME5CUVdkRE8yZENRVU5vUXl4MVEwRkJkVU03WjBKQlEzWkRMQ3RHUVVFclJqdG5Ra0ZETDBZc1lVRkJZVHRuUWtGRFlpeHBRMEZCYVVNN1owSkJRMnBETERaQ1FVRTJRanRuUWtGRE4wSXNORVpCUVRSR08yZENRVU0xUml4aFFVRmhPMmRDUVVOaUxHZERRVUZuUXp0blFrRkRhRU1zSzBKQlFTdENPMmRDUVVNdlFpdzRSa0ZCT0VZN1owSkJRemxHTEdGQlFXRTdaMEpCUTJJc1RVRkJUVHRuUWtGRFRpeGhRVUZoTzJkQ1FVTmlMRWxCUVVrc1QwRkJUeXhIUVVGSExFbEJRVWtzUTBGQlF5eExRVUZMTEVOQlFVTXNTVUZCU1N4RlFVRkZMRk5CUVZNc1EwRkJReXhEUVVGRE8yZENRVU14UXl4MVEwRkJkVU03WjBKQlEzWkRMRWxCUVVrc1IwRkJSeXhIUVVGSExFbEJRVWtzUTBGQlF5eExRVUZMTEVOQlFVTXNTVUZCU1N4RFFVRkRMRTFCUVUwc1JVRkJSU3hIUVVGSExFbEJRVWtzUTBGQlF5eEhRVUZITEVOQlFVTXNSVUZCUlN4RlFVRkZMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU03WjBKQlEzUkVMRTlCUVU4c1EwRkJReXhaUVVGWkxFTkJRVU1zVTBGQlV5eEZRVUZGTEVkQlFVY3NRMEZCUXl4RFFVRkRPMmRDUVVOeVF5eFZRVUZWTEVkQlFVY3NUMEZCVHl4RFFVRkRMRlZCUVZVc1EwRkJRenRuUWtGRGFFTXNUMEZCVHl4RFFVTk1MRlZCUVZVc1IwRkJSeXhIUVVGSExFZEJRVWNzVlVGQlZTeEZRVU0zUWl4VFFVRlRMRVZCUTFRc1ZVRkJWU3hGUVVOV0xGZEJRVmNzUlVGRFdDeFhRVUZYTEVOQlExb3NRMEZCUXp0blFrRkRSaXhQUVVGUExFOUJRVThzUTBGQlF6dGhRVU5vUWp0WlFVTkVMSFZDUVVGMVFqdFpRVVYyUWl4UFFVRlBMRU5CUTB3c1ZVRkJWU3hIUVVGSExFZEJRVWNzUjBGQlJ5eFZRVUZWTEVWQlF6ZENMRk5CUVZNc1JVRkRWQ3hWUVVGVkxFVkJRMVlzVjBGQlZ5eEZRVU5ZTEZkQlFWY3NRMEZEV2l4RFFVRkRPMWxCUTBZc1QwRkJUeXhKUVVGSkxFTkJRVU1zUzBGQlN5eERRVUZETEVsQlFVa3NSVUZCUlN4VFFVRlRMRU5CUVVNc1EwRkJRenRSUVVOeVF5eERRVUZETEVOQlFVTTdTVUZEU2l4RFFVRkRPMGxCUlVRc01rTkJRVEpETzBsQlF6TkRMRk5CUVZNc2QwSkJRWGRDTEVOQlF5OUNMRTFCUVUwc1JVRkRUaXhWUVVGclFpeEZRVU5zUWl4WlFVRnZRaXhGUVVOd1FpeFhRVUYzUWp0UlFVVjRRaXhKUVVORkxFTkJRVU1zVFVGQlRUdFpRVU5RTEVOQlFVTXNWVUZCVlR0WlFVTllMRU5CUVVNc1dVRkJXVHRaUVVOaUxGbEJRVmtzUzBGQlN5eFhRVUZYTEVWQlF6VkNPMWxCUTBFc1RVRkJUU3hKUVVGSkxFdEJRVXNzUTBGRFlqdHJRa0ZEVlN4TlFVRk5PM05DUVVOR0xGVkJRVlU3ZDBKQlExSXNXVUZCV1R0VFFVTXpRaXhEUVVOR0xFTkJRVU03VTBGRFNEdFJRVVZFTEhWRFFVRjFRenRSUVVOMlF5eE5RVUZOTEZGQlFWRXNSMEZCUnl4TlFVRk5MRU5CUVVNc2NVSkJRWEZDTEVOQlFVTXNUVUZCVFN4RlFVRkZMRmxCUVZrc1EwRkJReXhEUVVGRE8xRkJSWEJGTEc5R1FVRnZSanRSUVVOd1JpeEpRVU5GTEVOQlFVTXNVVUZCVVR0WlFVTlVMRU5CUVVNc1YwRkJWeXhEUVVGRExHbERRVUZwUXl4RFFVRkRMRkZCUVZFc1EwRkJReXhaUVVGWkxFTkJRVU1zUlVGRGNrVTdXVUZEUVN4UFFVRlBMRU5CUVVNc1MwRkJTeXhEUVVOWUxHMURRVUZ0UXl4RlFVTnVReXhWUVVGVkxFVkJRMVlzV1VGQldTeEZRVU5hTEUxQlFVMHNRMEZEVUN4RFFVRkRPMWxCUTBZc1QwRkJUenRUUVVOU08xRkJSVVFzSzBOQlFTdERPMUZCUXk5RExFbEJRVWtzYTBKQlFXdENMRU5CUVVNN1VVRkRka0lzVFVGQlRTeHBRa0ZCYVVJc1IwRkJSenRaUVVONFFpeEhRVUZITEVWQlFVVXNSMEZCUnl4RlFVRkZPMmRDUVVOU0xFOUJRVThzYTBKQlFXdENMRU5CUVVNN1dVRkROVUlzUTBGQlF6dFpRVU5FTEVkQlFVY3NSVUZCUlN4RFFVRkRMRXRCUVVzc1JVRkJSU3hGUVVGRk8yZENRVU5pTEd0Q1FVRnJRaXhIUVVGSExFdEJRVXNzUTBGQlF6dFpRVU0zUWl4RFFVRkRPMWxCUTBRc1ZVRkJWU3hGUVVGRkxFdEJRVXM3VTBGRGJFSXNRMEZCUXp0UlFVVkdMRzFFUVVGdFJEdFJRVU51UkN4TlFVRk5MR05CUVdNc1IwRkJSeXhSUVVGUkxFTkJRVU1zUTBGQlF5eERRVUZETEZGQlFWRXNRMEZCUXl4SFFVRkhMRU5CUVVNc1EwRkJReXhEUVVGRExHbENRVUZwUWl4RFFVRkRMRWRCUVVjc1EwRkJRenRSUVVOMlJTeE5RVUZOTEdOQlFXTXNSMEZCUnl4UlFVRlJMRU5CUVVNc1EwRkJReXhEUVVGRExGRkJRVkVzUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRMR2xDUVVGcFFpeERRVUZETEVkQlFVY3NRMEZCUXp0UlFVTjJSU3hKUVVGSkxHRkJRV0VzUjBGQlJ5eFJRVUZSTEVOQlFVTXNRMEZCUXl4RFFVRkRMRkZCUVZFc1EwRkJReXhMUVVGTExFTkJRVU1zUTBGQlF5eERRVUZETEd0Q1FVRnJRaXhEUVVGRE8xRkJSVzVGTEc5RlFVRnZSVHRSUVVOd1JTeHZRa0ZCYjBJN1VVRkRjRUlzVFVGQlRTeERRVUZETEdOQlFXTXNRMEZCUXl4TlFVRk5MRVZCUVVVc1dVRkJXU3hGUVVGRk8xbEJRekZETEZsQlFWa3NSVUZCUlN4SlFVRkpPMWxCUTJ4Q0xFZEJRVWNzUlVGQlJTeERRVUZETzJkQ1FVTktMRTlCUVU4N2IwSkJRMHdzU1VGQlNTeFpRVUZaTEVOQlFVTTdiMEpCUTJwQ0xFMUJRVTBzVjBGQlZ5eEhRVUZITERKQ1FVRXlRaXhEUVVNM1F5eFhRVUZYTEVOQlFVTXNXVUZCV1N4RFFVTjZRaXhEUVVGRE8yOUNRVU5HTEUxQlFVMHNkMEpCUVhkQ0xFZEJRVWNzUjBGQlJ5eFZRVUZWTEVsQlFVa3NXVUZCV1N4RlFVRkZMRU5CUVVNN2IwSkJRMnBGTEVsQlFVa3NWVUZCVlN4SFFVRkhMRVZCUVVVc1EwRkJRenR2UWtGRmNFSXNjVUpCUVhGQ08yOUNRVU55UWl4SlFVRkpMRU5CUVVNc1VVRkJVU3hGUVVGRk8zZENRVU5pTEhkQ1FVRjNRanQzUWtGRGVFSXNXVUZCV1N4SFFVRkhMR3RDUVVGclFpeERRVUZETzNGQ1FVTnVRenQ1UWtGQlRTeEpRVUZKTEdOQlFXTXNSVUZCUlR0M1FrRkRla0lzZFVKQlFYVkNPM2RDUVVOMlFpeFpRVUZaTEVkQlFVY3NZMEZCWXl4RFFVRkRMRWxCUVVrc1EwRkJReXhKUVVGSkxFTkJRVU1zUTBGQlF6dHhRa0ZETVVNN2VVSkJRVTBzU1VGQlNTeFBRVUZQTEVsQlFVa3NVVUZCVVN4RlFVRkZPM2RDUVVNNVFpeHRRa0ZCYlVJN2QwSkJRMjVDTEZsQlFWa3NSMEZCUnl4aFFVRmhMRU5CUVVNN2NVSkJRemxDTzNsQ1FVRk5PM2RDUVVOTUxFOUJRVThzUTBGQlF5eExRVUZMTEVOQlExZ3NNa0pCUVRKQ0xIZENRVUYzUWl4blEwRkJaME1zUTBGRGNFWXNRMEZCUXp0M1FrRkRSaXhSUVVGUkxFTkJRMDRzZDBKQlFYZENMRVZCUTNoQ0xFVkJRVVVzUlVGRFJpeFZRVUZWTEVWQlExWXNWMEZCVnl4RFFVRkRMRlZCUVZVc1JVRkRkRUlzVjBGQlZ5eEZRVU5ZTEZkQlFWY3NRMEZEV2l4RFFVRkRPM2RDUVVOR0xFOUJRVTg3Y1VKQlExSTdiMEpCUlVRc0swUkJRU3RFTzI5Q1FVTXZSQ3d5UkVGQk1rUTdiMEpCUXpORUxITkVRVUZ6UkR0dlFrRkRkRVFzYTBWQlFXdEZPMjlDUVVOc1JTeEpRVUZKTEU5QlFVOHNXVUZCV1N4TFFVRkxMRlZCUVZVc1JVRkJSVHQzUWtGRGRFTXNTVUZCU1N4WFFVRlhMRU5CUVVNc1pVRkJaU3hGUVVGRk96UkNRVU12UWl4UlFVRlJMRU5CUTA0c2QwSkJRWGRDTEVWQlEzaENMRmxCUVZrc1JVRkRXaXhWUVVGVkxFVkJRMVlzVjBGQlZ5eERRVUZETEZsQlFWa3NSVUZEZUVJc1YwRkJWeXhGUVVOWUxGZEJRVmNzUTBGRFdpeERRVUZETzNsQ1FVTklPM2RDUVVORUxFMUJRVTBzTWtKQlFUSkNMRWRCUVVjc2EwSkJRV3RDTEVOQlEzQkVMRlZCUVZVc1JVRkRWaXhaUVVGWkxFVkJRMW9zV1VGQldTeEZRVU5hTEZkQlFWY3NRMEZEV2l4RFFVRkRPM2RDUVVOR0xEUkdRVUUwUmp0M1FrRkROVVlzTUVkQlFUQkhPM2RDUVVNeFJ5eEpRVUZKTEZsQlFWa3NRMEZCUXl4VFFVRlRMRVZCUVVVN05FSkJRekZDTERKQ1FVRXlRaXhEUVVGRExGTkJRVk1zUjBGQlJ5eFpRVUZaTEVOQlFVTXNVMEZCVXl4RFFVRkRPelJDUVVNdlJDeEpRVUZKTEZsQlFWa3NRMEZCUXl4VFFVRlRMRU5CUVVNc1YwRkJWeXhGUVVGRk8yZERRVU4wUXl3eVFrRkJNa0lzUTBGQlF5eFRRVUZUTEVOQlFVTXNWMEZCVnp0dlEwRkRMME1zV1VGQldTeERRVUZETEZOQlFWTXNRMEZCUXl4WFFVRlhMRU5CUVVNN05rSkJRM1JETzNsQ1FVTkdPM2RDUVVORUxFOUJRVThzTWtKQlFUSkNMRU5CUVVNN2NVSkJRM0JETzNsQ1FVRk5MRWxCUTB3c1QwRkJUeXhaUVVGWkxFdEJRVXNzVVVGQlVUdDNRa0ZEYUVNc1YwRkJWeXhEUVVGRExGTkJRVk03ZDBKQlEzSkNMRmRCUVZjc1EwRkJReXhMUVVGTExFZEJRVWNzUTBGQlF5eEZRVU55UWp0M1FrRkRRU3hQUVVGUExGbEJRVmtzUTBGQlF6dHhRa0ZEY2tJN2VVSkJRVTA3ZDBKQlEwd3NVVUZCVVN4RFFVTk9MSGRDUVVGM1FpeEZRVU40UWl4WlFVRlpMRVZCUTFvc1ZVRkJWU3hGUVVOV0xGZEJRVmNzUTBGQlF5eEhRVUZITEVWQlEyWXNWMEZCVnl4RlFVTllMRmRCUVZjc1EwRkRXaXhEUVVGRE8zZENRVU5HTEU5QlFVOHNXVUZCV1N4RFFVRkRPM0ZDUVVOeVFqdG5Ra0ZEU0N4RFFVRkRMRU5CUVVNN1dVRkRTaXhEUVVGRExFTkJRVU1zUlVGQlJUdFpRVU5LTEVkQlFVY3NSVUZCUlN4RFFVRkRPMmRDUVVOS0xFOUJRVThzVlVGQlZTeExRVUZMTzI5Q1FVTndRaXhOUVVGTkxGZEJRVmNzUjBGQlJ5d3lRa0ZCTWtJc1EwRkROME1zVjBGQlZ5eERRVUZETEZsQlFWa3NRMEZEZWtJc1EwRkJRenR2UWtGRFJpeE5RVUZOTEhkQ1FVRjNRaXhIUVVGSExFZEJRVWNzVlVGQlZTeEpRVUZKTEZsQlFWa3NSVUZCUlN4RFFVRkRPMjlDUVVOcVJTeEpRVUZKTEZkQlFWY3NRMEZCUXp0dlFrRkRhRUlzU1VGQlNTeFZRVUZWTEVkQlFVY3NSVUZCUlN4RFFVRkRPMjlDUVVWd1FpeHZSRUZCYjBRN2IwSkJRM0JFTEVsQlEwVXNWMEZCVnl4RFFVRkRMRmRCUVZjN2QwSkJRM1pDTEVOQlFVTXNUMEZCVHl4aFFVRmhMRXRCUVVzc1ZVRkJWVHMwUWtGRGJFTXNUMEZCVHl4aFFVRmhMRXRCUVVzc1VVRkJVU3hEUVVGRExFVkJRM0JETzNkQ1FVTkJMRkZCUVZFc1EwRkRUaXgzUWtGQmQwSXNSVUZEZUVJc1MwRkJTeXhGUVVOTUxGVkJRVlVzUlVGRFZpeFhRVUZYTEVOQlFVTXNZVUZCWVN4RlFVTjZRaXhYUVVGWExFVkJRMWdzVjBGQlZ5eERRVU5hTEVOQlFVTTdkMEpCUTBZc1QwRkJUeXhMUVVGTExFTkJRVU03Y1VKQlEyUTdiMEpCUlVRc05FTkJRVFJETzI5Q1FVTTFReXhKUVVGSkxHTkJRV01zUlVGQlJUdDNRa0ZEYkVJc2RVSkJRWFZDTzNkQ1FVTjJRaXhYUVVGWExFZEJRVWNzWTBGQll5eERRVUZETEVsQlFVa3NRMEZCUXl4SlFVRkpMRVZCUVVVc1MwRkJTeXhEUVVGRExFTkJRVU03ZDBKQlF5OURMRWxCUVVrN05FSkJRMFlzU1VGQlNTeEpRVUZKTEVOQlFVTXNXVUZCV1N4RFFVRkRMRk5CUVZNc1EwRkJReXhGUVVGRk8yZERRVU01UWl4VlFVRlZMRWRCUVVjc1NVRkJTU3hEUVVGRExGVkJRVlVzUTBGQlF6czJRa0ZEYUVNN2VVSkJRMFk3ZDBKQlFVTXNUMEZCVHl4TFFVRkxMRVZCUVVVN05FSkJRMlFzVDBGQlR5eERRVUZETEVsQlFVa3NRMEZCUXl4dlEwRkJiME1zUTBGQlF5eERRVUZET3pSQ1FVTnVSQ3hWUVVGVkxFZEJRVWNzUlVGQlJTeERRVUZETzNsQ1FVTnFRanR4UWtGRFJqdDVRa0ZCVFN4SlFVRkpMRTlCUVU4c1NVRkJTU3hSUVVGUkxFVkJRVVU3ZDBKQlF6bENMRXRCUVVzc1IwRkJSeXhKUVVGSkxFTkJRVU03ZDBKQlEySXNTVUZCU1N4TlFVRk5MRU5CUVVNc1lVRkJZU3hEUVVGRExFbEJRVWtzUTBGQlF5eEZRVUZGT3pSQ1FVTTVRaXhOUVVGTkxFTkJRVU1zWTBGQll5eERRVUZETEVsQlFVa3NSVUZCUlN4WlFVRlpMRVZCUVVVN1owTkJRM2hETEV0QlFVczdOa0pCUTA0c1EwRkJReXhEUVVGRE8zbENRVU5LT3paQ1FVRk5PelJDUVVOTUxHRkJRV0VzUjBGQlJ5eExRVUZMTEVOQlFVTTdlVUpCUTNaQ08zZENRVU5FTEZkQlFWY3NSMEZCUnl4TFFVRkxMRU5CUVVNN2QwSkJRM0JDTEV0QlFVc3NSMEZCUnl4TFFVRkxMRU5CUVVNN2NVSkJRMlk3ZVVKQlFVMDdkMEpCUTB3c1QwRkJUeXhEUVVGRExFdEJRVXNzUTBGRFdDd3lRa0ZCTWtJc2QwSkJRWGRDTEdkRFFVRm5ReXhEUVVOd1JpeERRVUZETzNkQ1FVTkdMRkZCUVZFc1EwRkRUaXgzUWtGQmQwSXNSVUZEZUVJc1MwRkJTeXhGUVVOTUxGVkJRVlVzUlVGRFZpeFhRVUZYTEVOQlFVTXNWVUZCVlN4RlFVTjBRaXhYUVVGWExFVkJRMWdzVjBGQlZ5eERRVU5hTEVOQlFVTTdkMEpCUTBZc1QwRkJUeXhMUVVGTExFTkJRVU03Y1VKQlEyUTdiMEpCUTBRc1VVRkJVU3hEUVVOT0xIZENRVUYzUWl4RlFVTjRRaXhMUVVGTExFVkJRMHdzVlVGQlZTeEZRVU5XTEZkQlFWY3NRMEZCUXl4SFFVRkhMRVZCUTJZc1YwRkJWeXhGUVVOWUxGZEJRVmNzUTBGRFdpeERRVUZETzI5Q1FVTkdMRTlCUVU4c1YwRkJWeXhEUVVGRE8yZENRVU55UWl4RFFVRkRMRU5CUVVNN1dVRkRTaXhEUVVGRExFTkJRVU1zUlVGQlJUdFRRVU5NTEVOQlFVTXNRMEZCUXp0SlFVTk1MRU5CUVVNN1NVRkZSQ3hUUVVGVExHZENRVUZuUWl4RFFVTjJRaXhOUVVGWExFVkJRMWdzWjBKQlFYZENMRVZCUTNoQ0xGZEJRWGRDTzFGQlJYaENMR2RHUVVGblJqdFJRVU5vUml4M1EwRkJkME03VVVGRGVFTXNTVUZCU1N4elFrRkJaME1zUTBGQlF6dFJRVU55UXl4SlFVRkpMRmRCUVZjc1EwRkJReXh6UWtGQmMwSXNTMEZCU3l4SlFVRkpMRVZCUVVVN1dVRkRMME1zYzBKQlFYTkNMRWRCUVVjc1JVRkJSU3hEUVVGRE8xTkJRemRDTzJGQlFVMHNTVUZCU1N4WFFVRlhMRU5CUVVNc2MwSkJRWE5DTEVOQlFVTXNUVUZCVFN4TFFVRkxMRU5CUVVNc1JVRkJSVHRaUVVNeFJDeHpRa0ZCYzBJc1IwRkJSeXhOUVVGTkxFTkJRVU1zWjBKQlFXZENMRU5CUVVNc1RVRkJUU3hEUVVGRExFTkJRVU03VTBGRE1VUTdZVUZCVFR0WlFVTk1MSE5DUVVGelFpeEhRVUZITEZkQlFWY3NRMEZCUXl4elFrRkJjMElzUTBGQlF6dFRRVU0zUkR0UlFVTkVMRXRCUVVzc1RVRkJUU3haUVVGWkxFbEJRVWtzYzBKQlFYTkNMRVZCUVVVN1dVRkRha1FzU1VGQlNTeFhRVUZYTEVOQlFVTXNhMEpCUVd0Q0xFTkJRVU1zVVVGQlVTeERRVUZETEZsQlFWa3NRMEZCUXl4RlFVRkZPMmRDUVVONlJDeFRRVUZUTzJGQlExWTdXVUZEUkN4blJVRkJaMFU3V1VGRGFFVXNjMFJCUVhORU8xbEJRM1JFTEVsQlEwVXNWMEZCVnl4RFFVRkRMRk5CUVZNN1owSkJRM0pDTEZkQlFWY3NRMEZCUXl4TFFVRkxMRWRCUVVjc1EwRkJRenRuUWtGRGNrSXNVVUZCVVN4RFFVRkRMRTFCUVUwc1JVRkJSU3haUVVGWkxFTkJRVU03WjBKQlF6bENMRmxCUVZrc1MwRkJTeXhYUVVGWExFVkJRelZDTzJkQ1FVTkJMRTFCUVUwc2JVSkJRVzFDTEVkQlFVY3NSMEZCUnl4blFrRkJaMElzU1VGQlNTeFpRVUZaTEVWQlFVVXNRMEZCUXp0blFrRkRiRVVzVFVGQlRTeGpRVUZqTEVkQlFVY3NSVUZCUlN4SFFVRkhMRmRCUVZjc1JVRkJSU3hEUVVGRE8yZENRVU14UXl4alFVRmpMRU5CUVVNc1MwRkJTeXhIUVVGSExGZEJRVmNzUTBGQlF5eExRVUZMTEVkQlFVY3NRMEZCUXl4RFFVRkRPMmRDUVVNM1F5eGpRVUZqTEVOQlFVTXNjMEpCUVhOQ0xFZEJRVWNzUlVGQlJTeERRVUZETzJkQ1FVTXpReXhuUWtGQlowSXNRMEZEWkN4TlFVRk5MRU5CUVVNc1dVRkJXU3hEUVVGRExFVkJRM0JDTEcxQ1FVRnRRaXhGUVVOdVFpeGpRVUZqTEVOQlEyWXNRMEZCUXp0aFFVTklPMWxCUTBRc1NVRkJTVHRuUWtGRFJpeDNRa0ZCZDBJc1EwRkRkRUlzVFVGQlRTeEZRVU5PTEdkQ1FVRm5RaXhGUVVOb1FpeFpRVUZaTEVWQlExb3NWMEZCVnl4RFFVTmFMRU5CUVVNN1lVRkRTRHRaUVVGRExFOUJRVThzUzBGQlN5eEZRVUZGTzJkQ1FVTmtMRWxCUTBVc1MwRkJTeXhaUVVGWkxGTkJRVk03YjBKQlF6RkNMRXRCUVVzc1EwRkJReXhQUVVGUExFTkJRVU1zVVVGQlVTeERRVUZETERCRFFVRXdReXhEUVVGRExFVkJRMnhGTzI5Q1FVTkJMRTlCUVU4c1EwRkJReXhKUVVGSkxFTkJRMVlzWjBSQlFXZEVMR2RDUVVGblFpeEpRVUZKTEZsQlFWa3NSVUZCUlN4RFFVTnVSaXhEUVVGRE8ybENRVU5JTzNGQ1FVRk5PMjlDUVVOTUxHbENRVUZwUWl4RFFVRkRMRXRCUVVzc1JVRkJSU3hGUVVGRkxHZENRVUZuUWl4RlFVRkZMRmxCUVZrc1JVRkJSU3hEUVVGRExFTkJRVU03YVVKQlF6bEVPMkZCUTBZN1UwRkRSanRSUVVORUxFdEJRVXNzVFVGQlRTeFpRVUZaTEVsQlFVa3NWMEZCVnl4RFFVRkRMR2xEUVVGcFF5eEZRVUZGTzFsQlEzaEZMRWxCUVVrc1YwRkJWeXhEUVVGRExHdENRVUZyUWl4RFFVRkRMRkZCUVZFc1EwRkJReXhaUVVGWkxFTkJRVU1zUlVGQlJUdG5Ra0ZEZWtRc1UwRkJVenRoUVVOV08xbEJRMFFzU1VGQlNUdG5Ra0ZEUml4M1FrRkJkMElzUTBGRGRFSXNUVUZCVFN4RlFVTk9MR2RDUVVGblFpeEZRVU5vUWl4WlFVRlpMRVZCUTFvc1YwRkJWeXhEUVVOYUxFTkJRVU03WVVGRFNEdFpRVUZETEU5QlFVOHNTMEZCU3l4RlFVRkZPMmRDUVVOa0xHbENRVUZwUWl4RFFVRkRMRXRCUVVzc1JVRkJSU3hGUVVGRkxHZENRVUZuUWl4RlFVRkZMRmxCUVZrc1JVRkJSU3hEUVVGRExFTkJRVU03WVVGRE9VUTdVMEZEUmp0SlFVTklMRU5CUVVNN1NVRkZSQ3hOUVVGTkxGZEJRVmNzUjBGQlJ5eFZRVUZWTEU5QlFVOHNSVUZCUlN4eFFrRkJjVUk3VVVGRE1VUXNTVUZCU1N4UlFVRlJMRWRCUVVjc1JVRkJSU3hEUVVGRE8xRkJRMnhDTEcxRFFVRnRRenRSUVVOdVF5eE5RVUZOTEV0QlFVc3NSMEZCUnl4UlFVRlJMRU5CUVVNN1dVRkRja0lzY1VKQlFYRkNMRU5CUVVNc1QwRkJUeXhGUVVGRkxGRkJRVkVzUTBGQlF5eERRVUZETzFsQlJYcERMR3RDUVVGclFqdFpRVU5zUWl4UlFVRlJMRWRCUVVjc1JVRkJSU3hEUVVGRE8xRkJRMmhDTEVOQlFVTXNSVUZCUlN4SFFVRkhMRU5CUVVNc1EwRkJRenRSUVVWU0xFOUJRVThzVlVGQlZTeFBRVUZQTEVWQlFVVXNSMEZCUnp0WlFVTXpRaXh2UWtGQmIwSTdXVUZEY0VJc1VVRkJVU3hEUVVGRExFbEJRVWtzUTBGQlF5eEZRVUZGTEVsQlFVa3NSVUZCUlN4UFFVRlBMRVZCUVVVc1QwRkJUeXhGUVVGRkxFZEJRVWNzUlVGQlJTeERRVUZETEVOQlFVTTdXVUZETDBNc1MwRkJTeXhGUVVGRkxFTkJRVU03VVVGRFZpeERRVUZETEVOQlFVTTdTVUZEU2l4RFFVRkRMRU5CUVVNN1NVRkZSaXhOUVVGTkxFbEJRVWtzUjBGQlJ5eFhRVUZYTEVOQlFVTXNUMEZCVHl4RlFVRkZMRzlDUVVGdlFpeERRVUZETEVOQlFVTTdTVUZGZUVRc1UwRkJVeXhaUVVGWkxFTkJRVU1zYjBKQlFUSkRPMUZCUXk5RUxHbEZRVUZwUlR0UlFVTnFSU3c0UTBGQk9FTTdVVUZGT1VNc2VVUkJRWGxFTzFGQlEzcEVMR2xFUVVGcFJEdFJRVU5xUkN4dlFrRkJiMElzUTBGQlF5eFBRVUZQTEVOQlFVTXNWVUZCVlN4SlFVRkpPMWxCUTNwRExHZENRVUZuUWl4RFFVTmtMRWxCUVVrc1EwRkJReXhKUVVGSkxFTkJRVU1zVFVGQlRTeERRVUZETEVWQlEycENMRWxCUVVrc1EwRkJReXhuUWtGQlowSXNSVUZEY2tJc1NVRkJTU3hEUVVGRExGZEJRVmNzUTBGRGFrSXNRMEZCUXp0UlFVTktMRU5CUVVNc1EwRkJReXhEUVVGRE8wbEJRMHdzUTBGQlF6dEpRVVZFTEdkR1FVRm5SanRKUVVOb1JpeFBRVUZQTEZsQlFWa3NRMEZCUXp0QlFVTjBRaXhEUVVGREluMD0iLCIvKipcbiAqIFRpZXMgdG9nZXRoZXIgdGhlIHR3byBzZXBhcmF0ZSBuYXZpZ2F0aW9uIGV2ZW50cyB0aGF0IHRvZ2V0aGVyIGhvbGRzIGluZm9ybWF0aW9uIGFib3V0IGJvdGggcGFyZW50IGZyYW1lIGlkIGFuZCB0cmFuc2l0aW9uLXJlbGF0ZWQgYXR0cmlidXRlc1xuICovXG5leHBvcnQgY2xhc3MgUGVuZGluZ05hdmlnYXRpb24ge1xuICAgIG9uQmVmb3JlTmF2aWdhdGVFdmVudE5hdmlnYXRpb247XG4gICAgb25Db21taXR0ZWRFdmVudE5hdmlnYXRpb247XG4gICAgcmVzb2x2ZU9uQmVmb3JlTmF2aWdhdGVFdmVudE5hdmlnYXRpb247XG4gICAgcmVzb2x2ZU9uQ29tbWl0dGVkRXZlbnROYXZpZ2F0aW9uO1xuICAgIGNvbnN0cnVjdG9yKCkge1xuICAgICAgICB0aGlzLm9uQmVmb3JlTmF2aWdhdGVFdmVudE5hdmlnYXRpb24gPSBuZXcgUHJvbWlzZSgocmVzb2x2ZSkgPT4ge1xuICAgICAgICAgICAgdGhpcy5yZXNvbHZlT25CZWZvcmVOYXZpZ2F0ZUV2ZW50TmF2aWdhdGlvbiA9IHJlc29sdmU7XG4gICAgICAgIH0pO1xuICAgICAgICB0aGlzLm9uQ29tbWl0dGVkRXZlbnROYXZpZ2F0aW9uID0gbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHtcbiAgICAgICAgICAgIHRoaXMucmVzb2x2ZU9uQ29tbWl0dGVkRXZlbnROYXZpZ2F0aW9uID0gcmVzb2x2ZTtcbiAgICAgICAgfSk7XG4gICAgfVxuICAgIHJlc29sdmVkKCkge1xuICAgICAgICByZXR1cm4gUHJvbWlzZS5hbGwoW1xuICAgICAgICAgICAgdGhpcy5vbkJlZm9yZU5hdmlnYXRlRXZlbnROYXZpZ2F0aW9uLFxuICAgICAgICAgICAgdGhpcy5vbkNvbW1pdHRlZEV2ZW50TmF2aWdhdGlvbixcbiAgICAgICAgXSk7XG4gICAgfVxuICAgIC8qKlxuICAgICAqIEVpdGhlciByZXR1cm5zIG9yIHRpbWVzIG91dCBhbmQgcmV0dXJucyB1bmRlZmluZWQgb3JcbiAgICAgKiByZXR1cm5zIHRoZSByZXN1bHRzIGZyb20gcmVzb2x2ZWQoKSBhYm92ZVxuICAgICAqXG4gICAgICogQHBhcmFtIG1zXG4gICAgICovXG4gICAgYXN5bmMgcmVzb2x2ZWRXaXRoaW5UaW1lb3V0KG1zKSB7XG4gICAgICAgIGNvbnN0IHJlc29sdmVkID0gYXdhaXQgUHJvbWlzZS5yYWNlKFtcbiAgICAgICAgICAgIHRoaXMucmVzb2x2ZWQoKSxcbiAgICAgICAgICAgIG5ldyBQcm9taXNlKChyZXNvbHZlKSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIG1zKSksXG4gICAgICAgIF0pO1xuICAgICAgICByZXR1cm4gcmVzb2x2ZWQ7XG4gICAgfVxufVxuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pY0dWdVpHbHVaeTF1WVhacFoyRjBhVzl1TG1weklpd2ljMjkxY21ObFVtOXZkQ0k2SWlJc0luTnZkWEpqWlhNaU9sc2lMaTR2TGk0dkxpNHZjM0pqTDJ4cFlpOXdaVzVrYVc1bkxXNWhkbWxuWVhScGIyNHVkSE1pWFN3aWJtRnRaWE1pT2x0ZExDSnRZWEJ3YVc1bmN5STZJa0ZCUlVFN08wZEJSVWM3UVVGRFNDeE5RVUZOTEU5QlFVOHNhVUpCUVdsQ08wbEJRMW9zSzBKQlFTdENMRU5CUVhOQ08wbEJRM0pFTERCQ1FVRXdRaXhEUVVGelFqdEpRVU42UkN4elEwRkJjME1zUTBGQlowTTdTVUZEZEVVc2FVTkJRV2xETEVOQlFXZERPMGxCUTNoRk8xRkJRMFVzU1VGQlNTeERRVUZETEN0Q1FVRXJRaXhIUVVGSExFbEJRVWtzVDBGQlR5eERRVUZETEVOQlFVTXNUMEZCVHl4RlFVRkZMRVZCUVVVN1dVRkROMFFzU1VGQlNTeERRVUZETEhORFFVRnpReXhIUVVGSExFOUJRVThzUTBGQlF6dFJRVU40UkN4RFFVRkRMRU5CUVVNc1EwRkJRenRSUVVOSUxFbEJRVWtzUTBGQlF5d3dRa0ZCTUVJc1IwRkJSeXhKUVVGSkxFOUJRVThzUTBGQlF5eERRVUZETEU5QlFVOHNSVUZCUlN4RlFVRkZPMWxCUTNoRUxFbEJRVWtzUTBGQlF5eHBRMEZCYVVNc1IwRkJSeXhQUVVGUExFTkJRVU03VVVGRGJrUXNRMEZCUXl4RFFVRkRMRU5CUVVNN1NVRkRUQ3hEUVVGRE8wbEJRMDBzVVVGQlVUdFJRVU5pTEU5QlFVOHNUMEZCVHl4RFFVRkRMRWRCUVVjc1EwRkJRenRaUVVOcVFpeEpRVUZKTEVOQlFVTXNLMEpCUVN0Q08xbEJRM0JETEVsQlFVa3NRMEZCUXl3d1FrRkJNRUk3VTBGRGFFTXNRMEZCUXl4RFFVRkRPMGxCUTB3c1EwRkJRenRKUVVWRU96czdPenRQUVV0SE8wbEJRMGtzUzBGQlN5eERRVUZETEhGQ1FVRnhRaXhEUVVGRExFVkJRVVU3VVVGRGJrTXNUVUZCVFN4UlFVRlJMRWRCUVVjc1RVRkJUU3hQUVVGUExFTkJRVU1zU1VGQlNTeERRVUZETzFsQlEyeERMRWxCUVVrc1EwRkJReXhSUVVGUkxFVkJRVVU3V1VGRFppeEpRVUZKTEU5QlFVOHNRMEZCUXl4RFFVRkRMRTlCUVU4c1JVRkJSU3hGUVVGRkxFTkJRVU1zVlVGQlZTeERRVUZETEU5QlFVOHNSVUZCUlN4RlFVRkZMRU5CUVVNc1EwRkJRenRUUVVOc1JDeERRVUZETEVOQlFVTTdVVUZEU0N4UFFVRlBMRkZCUVZFc1EwRkJRenRKUVVOc1FpeERRVUZETzBOQlEwWWlmUT09IiwiLyoqXG4gKiBUaWVzIHRvZ2V0aGVyIHRoZSB0d28gc2VwYXJhdGUgZXZlbnRzIHRoYXQgdG9nZXRoZXIgaG9sZHMgaW5mb3JtYXRpb24gYWJvdXQgYm90aCByZXF1ZXN0IGhlYWRlcnMgYW5kIGJvZHlcbiAqL1xuZXhwb3J0IGNsYXNzIFBlbmRpbmdSZXF1ZXN0IHtcbiAgICBvbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHM7XG4gICAgb25CZWZvcmVTZW5kSGVhZGVyc0V2ZW50RGV0YWlscztcbiAgICByZXNvbHZlT25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzO1xuICAgIHJlc29sdmVPbkJlZm9yZVNlbmRIZWFkZXJzRXZlbnREZXRhaWxzO1xuICAgIGNvbnN0cnVjdG9yKCkge1xuICAgICAgICB0aGlzLm9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscyA9IG5ldyBQcm9taXNlKChyZXNvbHZlKSA9PiB7XG4gICAgICAgICAgICB0aGlzLnJlc29sdmVPbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHMgPSByZXNvbHZlO1xuICAgICAgICB9KTtcbiAgICAgICAgdGhpcy5vbkJlZm9yZVNlbmRIZWFkZXJzRXZlbnREZXRhaWxzID0gbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHtcbiAgICAgICAgICAgIHRoaXMucmVzb2x2ZU9uQmVmb3JlU2VuZEhlYWRlcnNFdmVudERldGFpbHMgPSByZXNvbHZlO1xuICAgICAgICB9KTtcbiAgICB9XG4gICAgcmVzb2x2ZWQoKSB7XG4gICAgICAgIHJldHVybiBQcm9taXNlLmFsbChbXG4gICAgICAgICAgICB0aGlzLm9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscyxcbiAgICAgICAgICAgIHRoaXMub25CZWZvcmVTZW5kSGVhZGVyc0V2ZW50RGV0YWlscyxcbiAgICAgICAgXSk7XG4gICAgfVxuICAgIC8qKlxuICAgICAqIEVpdGhlciByZXR1cm5zIG9yIHRpbWVzIG91dCBhbmQgcmV0dXJucyB1bmRlZmluZWQgb3JcbiAgICAgKiByZXR1cm5zIHRoZSByZXN1bHRzIGZyb20gcmVzb2x2ZWQoKSBhYm92ZVxuICAgICAqXG4gICAgICogQHBhcmFtIG1zXG4gICAgICovXG4gICAgYXN5bmMgcmVzb2x2ZWRXaXRoaW5UaW1lb3V0KG1zKSB7XG4gICAgICAgIGNvbnN0IHJlc29sdmVkID0gYXdhaXQgUHJvbWlzZS5yYWNlKFtcbiAgICAgICAgICAgIHRoaXMucmVzb2x2ZWQoKSxcbiAgICAgICAgICAgIG5ldyBQcm9taXNlKChyZXNvbHZlKSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIG1zKSksXG4gICAgICAgIF0pO1xuICAgICAgICByZXR1cm4gcmVzb2x2ZWQ7XG4gICAgfVxufVxuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pY0dWdVpHbHVaeTF5WlhGMVpYTjBMbXB6SWl3aWMyOTFjbU5sVW05dmRDSTZJaUlzSW5OdmRYSmpaWE1pT2xzaUxpNHZMaTR2TGk0dmMzSmpMMnhwWWk5d1pXNWthVzVuTFhKbGNYVmxjM1F1ZEhNaVhTd2libUZ0WlhNaU9sdGRMQ0p0WVhCd2FXNW5jeUk2SWtGQlMwRTdPMGRCUlVjN1FVRkRTQ3hOUVVGTkxFOUJRVThzWTBGQll6dEpRVU5VTERKQ1FVRXlRaXhEUVVGcFJEdEpRVU0xUlN3clFrRkJLMElzUTBGQmNVUTdTVUZETjBZc2EwTkJRV3RETEVOQlJTOUNPMGxCUTBnc2MwTkJRWE5ETEVOQlJXNURPMGxCUTFZN1VVRkRSU3hKUVVGSkxFTkJRVU1zTWtKQlFUSkNMRWRCUVVjc1NVRkJTU3hQUVVGUExFTkJRVU1zUTBGQlF5eFBRVUZQTEVWQlFVVXNSVUZCUlR0WlFVTjZSQ3hKUVVGSkxFTkJRVU1zYTBOQlFXdERMRWRCUVVjc1QwRkJUeXhEUVVGRE8xRkJRM0JFTEVOQlFVTXNRMEZCUXl4RFFVRkRPMUZCUTBnc1NVRkJTU3hEUVVGRExDdENRVUVyUWl4SFFVRkhMRWxCUVVrc1QwRkJUeXhEUVVGRExFTkJRVU1zVDBGQlR5eEZRVUZGTEVWQlFVVTdXVUZETjBRc1NVRkJTU3hEUVVGRExITkRRVUZ6UXl4SFFVRkhMRTlCUVU4c1EwRkJRenRSUVVONFJDeERRVUZETEVOQlFVTXNRMEZCUXp0SlFVTk1MRU5CUVVNN1NVRkRUU3hSUVVGUk8xRkJRMklzVDBGQlR5eFBRVUZQTEVOQlFVTXNSMEZCUnl4RFFVRkRPMWxCUTJwQ0xFbEJRVWtzUTBGQlF5d3lRa0ZCTWtJN1dVRkRhRU1zU1VGQlNTeERRVUZETEN0Q1FVRXJRanRUUVVOeVF5eERRVUZETEVOQlFVTTdTVUZEVEN4RFFVRkRPMGxCUlVRN096czdPMDlCUzBjN1NVRkRTU3hMUVVGTExFTkJRVU1zY1VKQlFYRkNMRU5CUVVNc1JVRkJSVHRSUVVOdVF5eE5RVUZOTEZGQlFWRXNSMEZCUnl4TlFVRk5MRTlCUVU4c1EwRkJReXhKUVVGSkxFTkJRVU03V1VGRGJFTXNTVUZCU1N4RFFVRkRMRkZCUVZFc1JVRkJSVHRaUVVObUxFbEJRVWtzVDBGQlR5eERRVUZETEVOQlFVTXNUMEZCVHl4RlFVRkZMRVZCUVVVc1EwRkJReXhWUVVGVkxFTkJRVU1zVDBGQlR5eEZRVUZGTEVWQlFVVXNRMEZCUXl4RFFVRkRPMU5CUTJ4RUxFTkJRVU1zUTBGQlF6dFJRVU5JTEU5QlFVOHNVVUZCVVN4RFFVRkRPMGxCUTJ4Q0xFTkJRVU03UTBGRFJpSjkiLCJpbXBvcnQgeyBSZXNwb25zZUJvZHlMaXN0ZW5lciB9IGZyb20gXCIuL3Jlc3BvbnNlLWJvZHktbGlzdGVuZXJcIjtcbi8qKlxuICogVGllcyB0b2dldGhlciB0aGUgdHdvIHNlcGFyYXRlIGV2ZW50cyB0aGF0IHRvZ2V0aGVyIGhvbGRzIGluZm9ybWF0aW9uIGFib3V0IGJvdGggcmVzcG9uc2UgaGVhZGVycyBhbmQgYm9keVxuICovXG5leHBvcnQgY2xhc3MgUGVuZGluZ1Jlc3BvbnNlIHtcbiAgICBvbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHM7XG4gICAgb25Db21wbGV0ZWRFdmVudERldGFpbHM7XG4gICAgcmVzcG9uc2VCb2R5TGlzdGVuZXI7XG4gICAgcmVzb2x2ZU9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscztcbiAgICByZXNvbHZlT25Db21wbGV0ZWRFdmVudERldGFpbHM7XG4gICAgY29uc3RydWN0b3IoKSB7XG4gICAgICAgIHRoaXMub25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzID0gbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHtcbiAgICAgICAgICAgIHRoaXMucmVzb2x2ZU9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscyA9IHJlc29sdmU7XG4gICAgICAgIH0pO1xuICAgICAgICB0aGlzLm9uQ29tcGxldGVkRXZlbnREZXRhaWxzID0gbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHtcbiAgICAgICAgICAgIHRoaXMucmVzb2x2ZU9uQ29tcGxldGVkRXZlbnREZXRhaWxzID0gcmVzb2x2ZTtcbiAgICAgICAgfSk7XG4gICAgfVxuICAgIGFkZFJlc3BvbnNlUmVzcG9uc2VCb2R5TGlzdGVuZXIoZGV0YWlscykge1xuICAgICAgICB0aGlzLnJlc3BvbnNlQm9keUxpc3RlbmVyID0gbmV3IFJlc3BvbnNlQm9keUxpc3RlbmVyKGRldGFpbHMpO1xuICAgIH1cbiAgICByZXNvbHZlZCgpIHtcbiAgICAgICAgcmV0dXJuIFByb21pc2UuYWxsKFtcbiAgICAgICAgICAgIHRoaXMub25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzLFxuICAgICAgICAgICAgdGhpcy5vbkNvbXBsZXRlZEV2ZW50RGV0YWlscyxcbiAgICAgICAgXSk7XG4gICAgfVxuICAgIC8qKlxuICAgICAqIEVpdGhlciByZXR1cm5zIG9yIHRpbWVzIG91dCBhbmQgcmV0dXJucyB1bmRlZmluZWQgb3JcbiAgICAgKiByZXR1cm5zIHRoZSByZXN1bHRzIGZyb20gcmVzb2x2ZWQoKSBhYm92ZVxuICAgICAqXG4gICAgICogQHBhcmFtIG1zXG4gICAgICovXG4gICAgYXN5bmMgcmVzb2x2ZWRXaXRoaW5UaW1lb3V0KG1zKSB7XG4gICAgICAgIGNvbnN0IHJlc29sdmVkID0gYXdhaXQgUHJvbWlzZS5yYWNlKFtcbiAgICAgICAgICAgIHRoaXMucmVzb2x2ZWQoKSxcbiAgICAgICAgICAgIG5ldyBQcm9taXNlKChyZXNvbHZlKSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIG1zKSksXG4gICAgICAgIF0pO1xuICAgICAgICByZXR1cm4gcmVzb2x2ZWQ7XG4gICAgfVxufVxuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pY0dWdVpHbHVaeTF5WlhOd2IyNXpaUzVxY3lJc0luTnZkWEpqWlZKdmIzUWlPaUlpTENKemIzVnlZMlZ6SWpwYklpNHVMeTR1THk0dUwzTnlZeTlzYVdJdmNHVnVaR2x1WnkxeVpYTndiMjV6WlM1MGN5SmRMQ0p1WVcxbGN5STZXMTBzSW0xaGNIQnBibWR6SWpvaVFVRkpRU3hQUVVGUExFVkJRVVVzYjBKQlFXOUNMRVZCUVVVc1RVRkJUU3d3UWtGQk1FSXNRMEZCUXp0QlFVVm9SVHM3UjBGRlJ6dEJRVU5JTEUxQlFVMHNUMEZCVHl4bFFVRmxPMGxCUTFZc01rSkJRVEpDTEVOQlFXbEVPMGxCUXpWRkxIVkNRVUYxUWl4RFFVRTJRenRKUVVNM1JTeHZRa0ZCYjBJc1EwRkJkVUk3U1VGRE0wTXNhME5CUVd0RExFTkJSUzlDTzBsQlEwZ3NPRUpCUVRoQ0xFTkJSVE5DTzBsQlExWTdVVUZEUlN4SlFVRkpMRU5CUVVNc01rSkJRVEpDTEVkQlFVY3NTVUZCU1N4UFFVRlBMRU5CUVVNc1EwRkJReXhQUVVGUExFVkJRVVVzUlVGQlJUdFpRVU42UkN4SlFVRkpMRU5CUVVNc2EwTkJRV3RETEVkQlFVY3NUMEZCVHl4RFFVRkRPMUZCUTNCRUxFTkJRVU1zUTBGQlF5eERRVUZETzFGQlEwZ3NTVUZCU1N4RFFVRkRMSFZDUVVGMVFpeEhRVUZITEVsQlFVa3NUMEZCVHl4RFFVRkRMRU5CUVVNc1QwRkJUeXhGUVVGRkxFVkJRVVU3V1VGRGNrUXNTVUZCU1N4RFFVRkRMRGhDUVVFNFFpeEhRVUZITEU5QlFVOHNRMEZCUXp0UlFVTm9SQ3hEUVVGRExFTkJRVU1zUTBGQlF6dEpRVU5NTEVOQlFVTTdTVUZEVFN3clFrRkJLMElzUTBGRGNFTXNUMEZCT0VNN1VVRkZPVU1zU1VGQlNTeERRVUZETEc5Q1FVRnZRaXhIUVVGSExFbEJRVWtzYjBKQlFXOUNMRU5CUVVNc1QwRkJUeXhEUVVGRExFTkJRVU03U1VGRGFFVXNRMEZCUXp0SlFVTk5MRkZCUVZFN1VVRkRZaXhQUVVGUExFOUJRVThzUTBGQlF5eEhRVUZITEVOQlFVTTdXVUZEYWtJc1NVRkJTU3hEUVVGRExESkNRVUV5UWp0WlFVTm9ReXhKUVVGSkxFTkJRVU1zZFVKQlFYVkNPMU5CUXpkQ0xFTkJRVU1zUTBGQlF6dEpRVU5NTEVOQlFVTTdTVUZGUkRzN096czdUMEZMUnp0SlFVTkpMRXRCUVVzc1EwRkJReXh4UWtGQmNVSXNRMEZCUXl4RlFVRkZPMUZCUTI1RExFMUJRVTBzVVVGQlVTeEhRVUZITEUxQlFVMHNUMEZCVHl4RFFVRkRMRWxCUVVrc1EwRkJRenRaUVVOc1F5eEpRVUZKTEVOQlFVTXNVVUZCVVN4RlFVRkZPMWxCUTJZc1NVRkJTU3hQUVVGUExFTkJRVU1zUTBGQlF5eFBRVUZQTEVWQlFVVXNSVUZCUlN4RFFVRkRMRlZCUVZVc1EwRkJReXhQUVVGUExFVkJRVVVzUlVGQlJTeERRVUZETEVOQlFVTTdVMEZEYkVRc1EwRkJReXhEUVVGRE8xRkJRMGdzVDBGQlR5eFJRVUZSTEVOQlFVTTdTVUZEYkVJc1EwRkJRenREUVVOR0luMD0iLCJpbXBvcnQgeyBkaWdlc3RNZXNzYWdlIH0gZnJvbSBcIi4vc2hhMjU2XCI7XG5leHBvcnQgY2xhc3MgUmVzcG9uc2VCb2R5TGlzdGVuZXIge1xuICAgIHJlc3BvbnNlQm9keTtcbiAgICBjb250ZW50SGFzaDtcbiAgICByZXNvbHZlUmVzcG9uc2VCb2R5O1xuICAgIHJlc29sdmVDb250ZW50SGFzaDtcbiAgICBjb25zdHJ1Y3RvcihkZXRhaWxzKSB7XG4gICAgICAgIHRoaXMucmVzcG9uc2VCb2R5ID0gbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHtcbiAgICAgICAgICAgIHRoaXMucmVzb2x2ZVJlc3BvbnNlQm9keSA9IHJlc29sdmU7XG4gICAgICAgIH0pO1xuICAgICAgICB0aGlzLmNvbnRlbnRIYXNoID0gbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHtcbiAgICAgICAgICAgIHRoaXMucmVzb2x2ZUNvbnRlbnRIYXNoID0gcmVzb2x2ZTtcbiAgICAgICAgfSk7XG4gICAgICAgIC8vIFVzZWQgdG8gcGFyc2UgUmVzcG9uc2Ugc3RyZWFtXG4gICAgICAgIGNvbnN0IGZpbHRlciA9IGJyb3dzZXIud2ViUmVxdWVzdC5maWx0ZXJSZXNwb25zZURhdGEoZGV0YWlscy5yZXF1ZXN0SWQudG9TdHJpbmcoKSk7XG4gICAgICAgIGxldCByZXNwb25zZUJvZHkgPSBuZXcgVWludDhBcnJheSgpO1xuICAgICAgICBmaWx0ZXIub25kYXRhID0gKGV2ZW50KSA9PiB7XG4gICAgICAgICAgICBkaWdlc3RNZXNzYWdlKGV2ZW50LmRhdGEpLnRoZW4oKGRpZ2VzdCkgPT4ge1xuICAgICAgICAgICAgICAgIHRoaXMucmVzb2x2ZUNvbnRlbnRIYXNoKGRpZ2VzdCk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIGNvbnN0IGluY29taW5nID0gbmV3IFVpbnQ4QXJyYXkoZXZlbnQuZGF0YSk7XG4gICAgICAgICAgICBjb25zdCB0bXAgPSBuZXcgVWludDhBcnJheShyZXNwb25zZUJvZHkubGVuZ3RoICsgaW5jb21pbmcubGVuZ3RoKTtcbiAgICAgICAgICAgIHRtcC5zZXQocmVzcG9uc2VCb2R5KTtcbiAgICAgICAgICAgIHRtcC5zZXQoaW5jb21pbmcsIHJlc3BvbnNlQm9keS5sZW5ndGgpO1xuICAgICAgICAgICAgcmVzcG9uc2VCb2R5ID0gdG1wO1xuICAgICAgICAgICAgZmlsdGVyLndyaXRlKGV2ZW50LmRhdGEpO1xuICAgICAgICB9O1xuICAgICAgICBmaWx0ZXIub25zdG9wID0gKF9ldmVudCkgPT4ge1xuICAgICAgICAgICAgdGhpcy5yZXNvbHZlUmVzcG9uc2VCb2R5KHJlc3BvbnNlQm9keSk7XG4gICAgICAgICAgICBmaWx0ZXIuZGlzY29ubmVjdCgpO1xuICAgICAgICB9O1xuICAgIH1cbiAgICBhc3luYyBnZXRSZXNwb25zZUJvZHkoKSB7XG4gICAgICAgIHJldHVybiB0aGlzLnJlc3BvbnNlQm9keTtcbiAgICB9XG4gICAgYXN5bmMgZ2V0Q29udGVudEhhc2goKSB7XG4gICAgICAgIHJldHVybiB0aGlzLmNvbnRlbnRIYXNoO1xuICAgIH1cbn1cbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWNtVnpjRzl1YzJVdFltOWtlUzFzYVhOMFpXNWxjaTVxY3lJc0luTnZkWEpqWlZKdmIzUWlPaUlpTENKemIzVnlZMlZ6SWpwYklpNHVMeTR1THk0dUwzTnlZeTlzYVdJdmNtVnpjRzl1YzJVdFltOWtlUzFzYVhOMFpXNWxjaTUwY3lKZExDSnVZVzFsY3lJNlcxMHNJbTFoY0hCcGJtZHpJam9pUVVGRFFTeFBRVUZQTEVWQlFVVXNZVUZCWVN4RlFVRkZMRTFCUVUwc1ZVRkJWU3hEUVVGRE8wRkJSWHBETEUxQlFVMHNUMEZCVHl4dlFrRkJiMEk3U1VGRFpDeFpRVUZaTEVOQlFYTkNPMGxCUTJ4RExGZEJRVmNzUTBGQmEwSTdTVUZEZEVNc2JVSkJRVzFDTEVOQlFYRkRPMGxCUTNoRUxHdENRVUZyUWl4RFFVRm5RenRKUVVVeFJDeFpRVUZaTEU5QlFUaERPMUZCUTNoRUxFbEJRVWtzUTBGQlF5eFpRVUZaTEVkQlFVY3NTVUZCU1N4UFFVRlBMRU5CUVVNc1EwRkJReXhQUVVGUExFVkJRVVVzUlVGQlJUdFpRVU14UXl4SlFVRkpMRU5CUVVNc2JVSkJRVzFDTEVkQlFVY3NUMEZCVHl4RFFVRkRPMUZCUTNKRExFTkJRVU1zUTBGQlF5eERRVUZETzFGQlEwZ3NTVUZCU1N4RFFVRkRMRmRCUVZjc1IwRkJSeXhKUVVGSkxFOUJRVThzUTBGQlF5eERRVUZETEU5QlFVOHNSVUZCUlN4RlFVRkZPMWxCUTNwRExFbEJRVWtzUTBGQlF5eHJRa0ZCYTBJc1IwRkJSeXhQUVVGUExFTkJRVU03VVVGRGNFTXNRMEZCUXl4RFFVRkRMRU5CUVVNN1VVRkZTQ3huUTBGQlowTTdVVUZEYUVNc1RVRkJUU3hOUVVGTkxFZEJRVkVzVDBGQlR5eERRVUZETEZWQlFWVXNRMEZCUXl4clFrRkJhMElzUTBGRGRrUXNUMEZCVHl4RFFVRkRMRk5CUVZNc1EwRkJReXhSUVVGUkxFVkJRVVVzUTBGRGRFSXNRMEZCUXp0UlFVVlVMRWxCUVVrc1dVRkJXU3hIUVVGSExFbEJRVWtzVlVGQlZTeEZRVUZGTEVOQlFVTTdVVUZEY0VNc1RVRkJUU3hEUVVGRExFMUJRVTBzUjBGQlJ5eERRVUZETEV0QlFVc3NSVUZCUlN4RlFVRkZPMWxCUTNoQ0xHRkJRV0VzUTBGQlF5eExRVUZMTEVOQlFVTXNTVUZCU1N4RFFVRkRMRU5CUVVNc1NVRkJTU3hEUVVGRExFTkJRVU1zVFVGQlRTeEZRVUZGTEVWQlFVVTdaMEpCUTNoRExFbEJRVWtzUTBGQlF5eHJRa0ZCYTBJc1EwRkJReXhOUVVGTkxFTkJRVU1zUTBGQlF6dFpRVU5zUXl4RFFVRkRMRU5CUVVNc1EwRkJRenRaUVVOSUxFMUJRVTBzVVVGQlVTeEhRVUZITEVsQlFVa3NWVUZCVlN4RFFVRkRMRXRCUVVzc1EwRkJReXhKUVVGSkxFTkJRVU1zUTBGQlF6dFpRVU0xUXl4TlFVRk5MRWRCUVVjc1IwRkJSeXhKUVVGSkxGVkJRVlVzUTBGQlF5eFpRVUZaTEVOQlFVTXNUVUZCVFN4SFFVRkhMRkZCUVZFc1EwRkJReXhOUVVGTkxFTkJRVU1zUTBGQlF6dFpRVU5zUlN4SFFVRkhMRU5CUVVNc1IwRkJSeXhEUVVGRExGbEJRVmtzUTBGQlF5eERRVUZETzFsQlEzUkNMRWRCUVVjc1EwRkJReXhIUVVGSExFTkJRVU1zVVVGQlVTeEZRVUZGTEZsQlFWa3NRMEZCUXl4TlFVRk5MRU5CUVVNc1EwRkJRenRaUVVOMlF5eFpRVUZaTEVkQlFVY3NSMEZCUnl4RFFVRkRPMWxCUTI1Q0xFMUJRVTBzUTBGQlF5eExRVUZMTEVOQlFVTXNTMEZCU3l4RFFVRkRMRWxCUVVrc1EwRkJReXhEUVVGRE8xRkJRek5DTEVOQlFVTXNRMEZCUXp0UlFVVkdMRTFCUVUwc1EwRkJReXhOUVVGTkxFZEJRVWNzUTBGQlF5eE5RVUZOTEVWQlFVVXNSVUZCUlR0WlFVTjZRaXhKUVVGSkxFTkJRVU1zYlVKQlFXMUNMRU5CUVVNc1dVRkJXU3hEUVVGRExFTkJRVU03V1VGRGRrTXNUVUZCVFN4RFFVRkRMRlZCUVZVc1JVRkJSU3hEUVVGRE8xRkJRM1JDTEVOQlFVTXNRMEZCUXp0SlFVTktMRU5CUVVNN1NVRkZUU3hMUVVGTExFTkJRVU1zWlVGQlpUdFJRVU14UWl4UFFVRlBMRWxCUVVrc1EwRkJReXhaUVVGWkxFTkJRVU03U1VGRE0wSXNRMEZCUXp0SlFVVk5MRXRCUVVzc1EwRkJReXhqUVVGak8xRkJRM3BDTEU5QlFVOHNTVUZCU1N4RFFVRkRMRmRCUVZjc1EwRkJRenRKUVVNeFFpeERRVUZETzBOQlEwWWlmUT09IiwiLyoqXG4gKiBDb2RlIGZyb20gdGhlIGV4YW1wbGUgYXRcbiAqIGh0dHBzOi8vZGV2ZWxvcGVyLm1vemlsbGEub3JnL2VuLVVTL2RvY3MvV2ViL0FQSS9TdWJ0bGVDcnlwdG8vZGlnZXN0XG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkaWdlc3RNZXNzYWdlKG1zZ1VpbnQ4KSB7XG4gICAgY29uc3QgaGFzaEJ1ZmZlciA9IGF3YWl0IGNyeXB0by5zdWJ0bGUuZGlnZXN0KFwiU0hBLTI1NlwiLCBtc2dVaW50OCk7IC8vIGhhc2ggdGhlIG1lc3NhZ2VcbiAgICBjb25zdCBoYXNoQXJyYXkgPSBBcnJheS5mcm9tKG5ldyBVaW50OEFycmF5KGhhc2hCdWZmZXIpKTsgLy8gY29udmVydCBidWZmZXIgdG8gYnl0ZSBhcnJheVxuICAgIGNvbnN0IGhhc2hIZXggPSBoYXNoQXJyYXlcbiAgICAgICAgLm1hcCgoYikgPT4gYi50b1N0cmluZygxNikucGFkU3RhcnQoMiwgXCIwXCIpKVxuICAgICAgICAuam9pbihcIlwiKTsgLy8gY29udmVydCBieXRlcyB0byBoZXggc3RyaW5nXG4gICAgcmV0dXJuIGhhc2hIZXg7XG59XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2ljMmhoTWpVMkxtcHpJaXdpYzI5MWNtTmxVbTl2ZENJNklpSXNJbk52ZFhKalpYTWlPbHNpTGk0dkxpNHZMaTR2YzNKakwyeHBZaTl6YUdFeU5UWXVkSE1pWFN3aWJtRnRaWE1pT2x0ZExDSnRZWEJ3YVc1bmN5STZJa0ZCUVVFN096dEhRVWRITzBGQlJVZ3NUVUZCVFN4RFFVRkRMRXRCUVVzc1ZVRkJWU3hoUVVGaExFTkJRVU1zVVVGQmIwSTdTVUZEZEVRc1RVRkJUU3hWUVVGVkxFZEJRVWNzVFVGQlRTeE5RVUZOTEVOQlFVTXNUVUZCVFN4RFFVRkRMRTFCUVUwc1EwRkJReXhUUVVGVExFVkJRVVVzVVVGQlVTeERRVUZETEVOQlFVTXNRMEZCUXl4dFFrRkJiVUk3U1VGRGRrWXNUVUZCVFN4VFFVRlRMRWRCUVVjc1MwRkJTeXhEUVVGRExFbEJRVWtzUTBGQlF5eEpRVUZKTEZWQlFWVXNRMEZCUXl4VlFVRlZMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU1zSzBKQlFTdENPMGxCUTNwR0xFMUJRVTBzVDBGQlR5eEhRVUZITEZOQlFWTTdVMEZEZEVJc1IwRkJSeXhEUVVGRExFTkJRVU1zUTBGQlF5eEZRVUZGTEVWQlFVVXNRMEZCUXl4RFFVRkRMRU5CUVVNc1VVRkJVU3hEUVVGRExFVkJRVVVzUTBGQlF5eERRVUZETEZGQlFWRXNRMEZCUXl4RFFVRkRMRVZCUVVVc1IwRkJSeXhEUVVGRExFTkJRVU03VTBGRE0wTXNTVUZCU1N4RFFVRkRMRVZCUVVVc1EwRkJReXhEUVVGRExFTkJRVU1zT0VKQlFUaENPMGxCUXpORExFOUJRVThzVDBGQlR5eERRVUZETzBGQlEycENMRU5CUVVNaWZRPT0iLCJleHBvcnQgZnVuY3Rpb24gZW5jb2RlX3V0Zjgocykge1xuICAgIHJldHVybiB1bmVzY2FwZShlbmNvZGVVUklDb21wb25lbnQocykpO1xufVxuZXhwb3J0IGNvbnN0IGVzY2FwZVN0cmluZyA9IGZ1bmN0aW9uIChzdHIpIHtcbiAgICAvLyBDb252ZXJ0IHRvIHN0cmluZyBpZiBuZWNlc3NhcnlcbiAgICBpZiAodHlwZW9mIHN0ciAhPT0gXCJzdHJpbmdcIikge1xuICAgICAgICBzdHIgPSBTdHJpbmcoc3RyKTtcbiAgICB9XG4gICAgcmV0dXJuIGVuY29kZV91dGY4KHN0cik7XG59O1xuZXhwb3J0IGNvbnN0IGVzY2FwZVVybCA9IGZ1bmN0aW9uICh1cmwsIHN0cmlwRGF0YVVybERhdGEgPSB0cnVlKSB7XG4gICAgdXJsID0gZXNjYXBlU3RyaW5nKHVybCk7XG4gICAgLy8gZGF0YTpbPG1lZGlhdHlwZT5dWztiYXNlNjRdLDxkYXRhPlxuICAgIGlmICh1cmwuc3Vic3RyKDAsIDUpID09PSBcImRhdGE6XCIgJiZcbiAgICAgICAgc3RyaXBEYXRhVXJsRGF0YSAmJlxuICAgICAgICB1cmwuaW5kZXhPZihcIixcIikgPiAtMSkge1xuICAgICAgICB1cmwgPSB1cmwuc3Vic3RyKDAsIHVybC5pbmRleE9mKFwiLFwiKSArIDEpICsgXCI8ZGF0YS1zdHJpcHBlZD5cIjtcbiAgICB9XG4gICAgcmV0dXJuIHVybDtcbn07XG4vLyBCYXNlNjQgZW5jb2RpbmcsIGZvdW5kIG9uOlxuLy8gaHR0cHM6Ly9zdGFja292ZXJmbG93LmNvbS9xdWVzdGlvbnMvMTI3MTAwMDEvaG93LXRvLWNvbnZlcnQtdWludDgtYXJyYXktdG8tYmFzZTY0LWVuY29kZWQtc3RyaW5nLzI1NjQ0NDA5IzI1NjQ0NDA5XG5leHBvcnQgY29uc3QgVWludDhUb0Jhc2U2NCA9IGZ1bmN0aW9uICh1OEFycikge1xuICAgIGNvbnN0IENIVU5LX1NJWkUgPSAweDgwMDA7IC8vIGFyYml0cmFyeSBudW1iZXJcbiAgICBsZXQgaW5kZXggPSAwO1xuICAgIGNvbnN0IGxlbmd0aCA9IHU4QXJyLmxlbmd0aDtcbiAgICBsZXQgcmVzdWx0ID0gXCJcIjtcbiAgICBsZXQgc2xpY2U7XG4gICAgd2hpbGUgKGluZGV4IDwgbGVuZ3RoKSB7XG4gICAgICAgIHNsaWNlID0gdThBcnIuc3ViYXJyYXkoaW5kZXgsIE1hdGgubWluKGluZGV4ICsgQ0hVTktfU0laRSwgbGVuZ3RoKSk7XG4gICAgICAgIHJlc3VsdCArPSBTdHJpbmcuZnJvbUNoYXJDb2RlLmFwcGx5KG51bGwsIHNsaWNlKTtcbiAgICAgICAgaW5kZXggKz0gQ0hVTktfU0laRTtcbiAgICB9XG4gICAgcmV0dXJuIGJ0b2EocmVzdWx0KTtcbn07XG5leHBvcnQgY29uc3QgYm9vbFRvSW50ID0gZnVuY3Rpb24gKGJvb2wpIHtcbiAgICByZXR1cm4gYm9vbCA/IDEgOiAwO1xufTtcbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWMzUnlhVzVuTFhWMGFXeHpMbXB6SWl3aWMyOTFjbU5sVW05dmRDSTZJaUlzSW5OdmRYSmpaWE1pT2xzaUxpNHZMaTR2TGk0dmMzSmpMMnhwWWk5emRISnBibWN0ZFhScGJITXVkSE1pWFN3aWJtRnRaWE1pT2x0ZExDSnRZWEJ3YVc1bmN5STZJa0ZCUVVFc1RVRkJUU3hWUVVGVkxGZEJRVmNzUTBGQlF5eERRVUZETzBsQlF6TkNMRTlCUVU4c1VVRkJVU3hEUVVGRExHdENRVUZyUWl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU03UVVGRGVrTXNRMEZCUXp0QlFVVkVMRTFCUVUwc1EwRkJReXhOUVVGTkxGbEJRVmtzUjBGQlJ5eFZRVUZWTEVkQlFWRTdTVUZETlVNc2FVTkJRV2xETzBsQlEycERMRWxCUVVrc1QwRkJUeXhIUVVGSExFdEJRVXNzVVVGQlVTeEZRVUZGTzFGQlF6TkNMRWRCUVVjc1IwRkJSeXhOUVVGTkxFTkJRVU1zUjBGQlJ5eERRVUZETEVOQlFVTTdTMEZEYmtJN1NVRkZSQ3hQUVVGUExGZEJRVmNzUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXp0QlFVTXhRaXhEUVVGRExFTkJRVU03UVVGRlJpeE5RVUZOTEVOQlFVTXNUVUZCVFN4VFFVRlRMRWRCUVVjc1ZVRkRka0lzUjBGQlZ5eEZRVU5ZTEcxQ1FVRTBRaXhKUVVGSk8wbEJSV2hETEVkQlFVY3NSMEZCUnl4WlFVRlpMRU5CUVVNc1IwRkJSeXhEUVVGRExFTkJRVU03U1VGRGVFSXNjVU5CUVhGRE8wbEJRM0pETEVsQlEwVXNSMEZCUnl4RFFVRkRMRTFCUVUwc1EwRkJReXhEUVVGRExFVkJRVVVzUTBGQlF5eERRVUZETEV0QlFVc3NUMEZCVHp0UlFVTTFRaXhuUWtGQlowSTdVVUZEYUVJc1IwRkJSeXhEUVVGRExFOUJRVThzUTBGQlF5eEhRVUZITEVOQlFVTXNSMEZCUnl4RFFVRkRMRU5CUVVNc1JVRkRja0k3VVVGRFFTeEhRVUZITEVkQlFVY3NSMEZCUnl4RFFVRkRMRTFCUVUwc1EwRkJReXhEUVVGRExFVkJRVVVzUjBGQlJ5eERRVUZETEU5QlFVOHNRMEZCUXl4SFFVRkhMRU5CUVVNc1IwRkJSeXhEUVVGRExFTkJRVU1zUjBGQlJ5eHBRa0ZCYVVJc1EwRkJRenRMUVVNdlJEdEpRVU5FTEU5QlFVOHNSMEZCUnl4RFFVRkRPMEZCUTJJc1EwRkJReXhEUVVGRE8wRkJSVVlzTmtKQlFUWkNPMEZCUXpkQ0xIRklRVUZ4U0R0QlFVTnlTQ3hOUVVGTkxFTkJRVU1zVFVGQlRTeGhRVUZoTEVkQlFVY3NWVUZCVlN4TFFVRnBRanRKUVVOMFJDeE5RVUZOTEZWQlFWVXNSMEZCUnl4TlFVRk5MRU5CUVVNc1EwRkJReXh0UWtGQmJVSTdTVUZET1VNc1NVRkJTU3hMUVVGTExFZEJRVWNzUTBGQlF5eERRVUZETzBsQlEyUXNUVUZCVFN4TlFVRk5MRWRCUVVjc1MwRkJTeXhEUVVGRExFMUJRVTBzUTBGQlF6dEpRVU0xUWl4SlFVRkpMRTFCUVUwc1IwRkJSeXhGUVVGRkxFTkJRVU03U1VGRGFFSXNTVUZCU1N4TFFVRnBRaXhEUVVGRE8wbEJRM1JDTEU5QlFVOHNTMEZCU3l4SFFVRkhMRTFCUVUwc1JVRkJSVHRSUVVOeVFpeExRVUZMTEVkQlFVY3NTMEZCU3l4RFFVRkRMRkZCUVZFc1EwRkJReXhMUVVGTExFVkJRVVVzU1VGQlNTeERRVUZETEVkQlFVY3NRMEZCUXl4TFFVRkxMRWRCUVVjc1ZVRkJWU3hGUVVGRkxFMUJRVTBzUTBGQlF5eERRVUZETEVOQlFVTTdVVUZEY0VVc1RVRkJUU3hKUVVGSkxFMUJRVTBzUTBGQlF5eFpRVUZaTEVOQlFVTXNTMEZCU3l4RFFVRkRMRWxCUVVrc1JVRkJSU3hMUVVGTExFTkJRVU1zUTBGQlF6dFJRVU5xUkN4TFFVRkxMRWxCUVVrc1ZVRkJWU3hEUVVGRE8wdEJRM0pDTzBsQlEwUXNUMEZCVHl4SlFVRkpMRU5CUVVNc1RVRkJUU3hEUVVGRExFTkJRVU03UVVGRGRFSXNRMEZCUXl4RFFVRkRPMEZCUlVZc1RVRkJUU3hEUVVGRExFMUJRVTBzVTBGQlV5eEhRVUZITEZWQlFWVXNTVUZCWVR0SlFVTTVReXhQUVVGUExFbEJRVWtzUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU03UVVGRGRFSXNRMEZCUXl4RFFVRkRJbjA9IiwiLyogZXNsaW50LWRpc2FibGUgbm8tYml0d2lzZSAqL1xuLy8gZnJvbSBodHRwczovL2dpc3QuZ2l0aHViLmNvbS9qZWQvOTgyODgzI2dpc3Rjb21tZW50LTI0MDMzNjlcbmNvbnN0IGhleCA9IFtdO1xuZm9yIChsZXQgaSA9IDA7IGkgPCAyNTY7IGkrKykge1xuICAgIGhleFtpXSA9IChpIDwgMTYgPyBcIjBcIiA6IFwiXCIpICsgaS50b1N0cmluZygxNik7XG59XG5leHBvcnQgY29uc3QgbWFrZVVVSUQgPSAoKSA9PiB7XG4gICAgY29uc3QgciA9IGNyeXB0by5nZXRSYW5kb21WYWx1ZXMobmV3IFVpbnQ4QXJyYXkoMTYpKTtcbiAgICByWzZdID0gKHJbNl0gJiAweDBmKSB8IDB4NDA7XG4gICAgcls4XSA9IChyWzhdICYgMHgzZikgfCAweDgwO1xuICAgIHJldHVybiAoaGV4W3JbMF1dICtcbiAgICAgICAgaGV4W3JbMV1dICtcbiAgICAgICAgaGV4W3JbMl1dICtcbiAgICAgICAgaGV4W3JbM11dICtcbiAgICAgICAgXCItXCIgK1xuICAgICAgICBoZXhbcls0XV0gK1xuICAgICAgICBoZXhbcls1XV0gK1xuICAgICAgICBcIi1cIiArXG4gICAgICAgIGhleFtyWzZdXSArXG4gICAgICAgIGhleFtyWzddXSArXG4gICAgICAgIFwiLVwiICtcbiAgICAgICAgaGV4W3JbOF1dICtcbiAgICAgICAgaGV4W3JbOV1dICtcbiAgICAgICAgXCItXCIgK1xuICAgICAgICBoZXhbclsxMF1dICtcbiAgICAgICAgaGV4W3JbMTFdXSArXG4gICAgICAgIGhleFtyWzEyXV0gK1xuICAgICAgICBoZXhbclsxM11dICtcbiAgICAgICAgaGV4W3JbMTRdXSArXG4gICAgICAgIGhleFtyWzE1XV0pO1xufTtcbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWRYVnBaQzVxY3lJc0luTnZkWEpqWlZKdmIzUWlPaUlpTENKemIzVnlZMlZ6SWpwYklpNHVMeTR1THk0dUwzTnlZeTlzYVdJdmRYVnBaQzUwY3lKZExDSnVZVzFsY3lJNlcxMHNJbTFoY0hCcGJtZHpJam9pUVVGQlFTd3JRa0ZCSzBJN1FVRkZMMElzT0VSQlFUaEVPMEZCUXpsRUxFMUJRVTBzUjBGQlJ5eEhRVUZITEVWQlFVVXNRMEZCUXp0QlFVVm1MRXRCUVVzc1NVRkJTU3hEUVVGRExFZEJRVWNzUTBGQlF5eEZRVUZGTEVOQlFVTXNSMEZCUnl4SFFVRkhMRVZCUVVVc1EwRkJReXhGUVVGRkxFVkJRVVU3U1VGRE5VSXNSMEZCUnl4RFFVRkRMRU5CUVVNc1EwRkJReXhIUVVGSExFTkJRVU1zUTBGQlF5eEhRVUZITEVWQlFVVXNRMEZCUXl4RFFVRkRMRU5CUVVNc1IwRkJSeXhEUVVGRExFTkJRVU1zUTBGQlF5eEZRVUZGTEVOQlFVTXNSMEZCUnl4RFFVRkRMRU5CUVVNc1VVRkJVU3hEUVVGRExFVkJRVVVzUTBGQlF5eERRVUZETzBOQlF5OURPMEZCUlVRc1RVRkJUU3hEUVVGRExFMUJRVTBzVVVGQlVTeEhRVUZITEVkQlFVY3NSVUZCUlR0SlFVTXpRaXhOUVVGTkxFTkJRVU1zUjBGQlJ5eE5RVUZOTEVOQlFVTXNaVUZCWlN4RFFVRkRMRWxCUVVrc1ZVRkJWU3hEUVVGRExFVkJRVVVzUTBGQlF5eERRVUZETEVOQlFVTTdTVUZGY2tRc1EwRkJReXhEUVVGRExFTkJRVU1zUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhIUVVGSExFbEJRVWtzUTBGQlF5eEhRVUZITEVsQlFVa3NRMEZCUXp0SlFVTTFRaXhEUVVGRExFTkJRVU1zUTBGQlF5eERRVUZETEVkQlFVY3NRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExFZEJRVWNzU1VGQlNTeERRVUZETEVkQlFVY3NTVUZCU1N4RFFVRkRPMGxCUlRWQ0xFOUJRVThzUTBGRFRDeEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRE8xRkJRMVFzUjBGQlJ5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJRenRSUVVOVUxFZEJRVWNzUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNN1VVRkRWQ3hIUVVGSExFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRPMUZCUTFRc1IwRkJSenRSUVVOSUxFZEJRVWNzUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNN1VVRkRWQ3hIUVVGSExFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRPMUZCUTFRc1IwRkJSenRSUVVOSUxFZEJRVWNzUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNN1VVRkRWQ3hIUVVGSExFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRPMUZCUTFRc1IwRkJSenRSUVVOSUxFZEJRVWNzUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNN1VVRkRWQ3hIUVVGSExFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRPMUZCUTFRc1IwRkJSenRSUVVOSUxFZEJRVWNzUTBGQlF5eERRVUZETEVOQlFVTXNSVUZCUlN4RFFVRkRMRU5CUVVNN1VVRkRWaXhIUVVGSExFTkJRVU1zUTBGQlF5eERRVUZETEVWQlFVVXNRMEZCUXl4RFFVRkRPMUZCUTFZc1IwRkJSeXhEUVVGRExFTkJRVU1zUTBGQlF5eEZRVUZGTEVOQlFVTXNRMEZCUXp0UlFVTldMRWRCUVVjc1EwRkJReXhEUVVGRExFTkJRVU1zUlVGQlJTeERRVUZETEVOQlFVTTdVVUZEVml4SFFVRkhMRU5CUVVNc1EwRkJReXhEUVVGRExFVkJRVVVzUTBGQlF5eERRVUZETzFGQlExWXNSMEZCUnl4RFFVRkRMRU5CUVVNc1EwRkJReXhGUVVGRkxFTkJRVU1zUTBGQlF5eERRVU5ZTEVOQlFVTTdRVUZEU2l4RFFVRkRMRU5CUVVNaWZRPT0iLCIvLyBodHRwczovL3d3dy51bmljb2RlLm9yZy9yZXBvcnRzL3RyMzUvdHIzNS1kYXRlcy5odG1sI0RhdGVfRmllbGRfU3ltYm9sX1RhYmxlXG5leHBvcnQgY29uc3QgZGF0ZVRpbWVVbmljb2RlRm9ybWF0U3RyaW5nID0gXCJ5eXl5LU1NLWRkJ1QnSEg6bW06c3MuU1NTWFhcIjtcbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWMyTm9aVzFoTG1weklpd2ljMjkxY21ObFVtOXZkQ0k2SWlJc0luTnZkWEpqWlhNaU9sc2lMaTR2TGk0dmMzSmpMM05qYUdWdFlTNTBjeUpkTENKdVlXMWxjeUk2VzEwc0ltMWhjSEJwYm1keklqb2lRVUZKUVN3clJVRkJLMFU3UVVGREwwVXNUVUZCVFN4RFFVRkRMRTFCUVUwc01rSkJRVEpDTEVkQlFVY3NOa0pCUVRaQ0xFTkJRVU1pZlE9PSIsIi8vIFRoZSBtb2R1bGUgY2FjaGVcbnZhciBfX3dlYnBhY2tfbW9kdWxlX2NhY2hlX18gPSB7fTtcblxuLy8gVGhlIHJlcXVpcmUgZnVuY3Rpb25cbmZ1bmN0aW9uIF9fd2VicGFja19yZXF1aXJlX18obW9kdWxlSWQpIHtcblx0Ly8gQ2hlY2sgaWYgbW9kdWxlIGlzIGluIGNhY2hlXG5cdHZhciBjYWNoZWRNb2R1bGUgPSBfX3dlYnBhY2tfbW9kdWxlX2NhY2hlX19bbW9kdWxlSWRdO1xuXHRpZiAoY2FjaGVkTW9kdWxlICE9PSB1bmRlZmluZWQpIHtcblx0XHRyZXR1cm4gY2FjaGVkTW9kdWxlLmV4cG9ydHM7XG5cdH1cblx0Ly8gQ3JlYXRlIGEgbmV3IG1vZHVsZSAoYW5kIHB1dCBpdCBpbnRvIHRoZSBjYWNoZSlcblx0dmFyIG1vZHVsZSA9IF9fd2VicGFja19tb2R1bGVfY2FjaGVfX1ttb2R1bGVJZF0gPSB7XG5cdFx0Ly8gbm8gbW9kdWxlLmlkIG5lZWRlZFxuXHRcdC8vIG5vIG1vZHVsZS5sb2FkZWQgbmVlZGVkXG5cdFx0ZXhwb3J0czoge31cblx0fTtcblxuXHQvLyBFeGVjdXRlIHRoZSBtb2R1bGUgZnVuY3Rpb25cblx0X193ZWJwYWNrX21vZHVsZXNfX1ttb2R1bGVJZF0obW9kdWxlLCBtb2R1bGUuZXhwb3J0cywgX193ZWJwYWNrX3JlcXVpcmVfXyk7XG5cblx0Ly8gUmV0dXJuIHRoZSBleHBvcnRzIG9mIHRoZSBtb2R1bGVcblx0cmV0dXJuIG1vZHVsZS5leHBvcnRzO1xufVxuXG4iLCIvLyBkZWZpbmUgZ2V0dGVyIGZ1bmN0aW9ucyBmb3IgaGFybW9ueSBleHBvcnRzXG5fX3dlYnBhY2tfcmVxdWlyZV9fLmQgPSAoZXhwb3J0cywgZGVmaW5pdGlvbikgPT4ge1xuXHRmb3IodmFyIGtleSBpbiBkZWZpbml0aW9uKSB7XG5cdFx0aWYoX193ZWJwYWNrX3JlcXVpcmVfXy5vKGRlZmluaXRpb24sIGtleSkgJiYgIV9fd2VicGFja19yZXF1aXJlX18ubyhleHBvcnRzLCBrZXkpKSB7XG5cdFx0XHRPYmplY3QuZGVmaW5lUHJvcGVydHkoZXhwb3J0cywga2V5LCB7IGVudW1lcmFibGU6IHRydWUsIGdldDogZGVmaW5pdGlvbltrZXldIH0pO1xuXHRcdH1cblx0fVxufTsiLCJfX3dlYnBhY2tfcmVxdWlyZV9fLm8gPSAob2JqLCBwcm9wKSA9PiAoT2JqZWN0LnByb3RvdHlwZS5oYXNPd25Qcm9wZXJ0eS5jYWxsKG9iaiwgcHJvcCkpIiwiLy8gZGVmaW5lIF9fZXNNb2R1bGUgb24gZXhwb3J0c1xuX193ZWJwYWNrX3JlcXVpcmVfXy5yID0gKGV4cG9ydHMpID0+IHtcblx0aWYodHlwZW9mIFN5bWJvbCAhPT0gJ3VuZGVmaW5lZCcgJiYgU3ltYm9sLnRvU3RyaW5nVGFnKSB7XG5cdFx0T2JqZWN0LmRlZmluZVByb3BlcnR5KGV4cG9ydHMsIFN5bWJvbC50b1N0cmluZ1RhZywgeyB2YWx1ZTogJ01vZHVsZScgfSk7XG5cdH1cblx0T2JqZWN0LmRlZmluZVByb3BlcnR5KGV4cG9ydHMsICdfX2VzTW9kdWxlJywgeyB2YWx1ZTogdHJ1ZSB9KTtcbn07IiwiaW1wb3J0IHtcbiAgICBDb29raWVJbnN0cnVtZW50LFxuICAgIERuc0luc3RydW1lbnQsXG4gICAgSHR0cEluc3RydW1lbnQsXG4gICAgSmF2YXNjcmlwdEluc3RydW1lbnQsXG4gICAgTmF2aWdhdGlvbkluc3RydW1lbnRcbn0gZnJvbSBcIm9wZW53cG0td2ViZXh0LWluc3RydW1lbnRhdGlvblwiO1xuXG5pbXBvcnQgKiBhcyBsb2dnaW5nREIgZnJvbSBcIi4vbG9nZ2luZ2RiLmpzXCI7XG5pbXBvcnQge0NhbGxzdGFja0luc3RydW1lbnR9IGZyb20gXCIuL2NhbGxzdGFjay1pbnN0cnVtZW50LmpzXCI7XG5cbmFzeW5jIGZ1bmN0aW9uIG1haW4oKSB7XG4gIC8vIFJlYWQgdGhlIGJyb3dzZXIgY29uZmlndXJhdGlvbiBmcm9tIGZpbGVcbiAgbGV0IGZpbGVuYW1lID0gXCJicm93c2VyX3BhcmFtcy5qc29uXCI7XG4gIGxldCBjb25maWcgPSBhd2FpdCBicm93c2VyLnByb2ZpbGVEaXJJTy5yZWFkRmlsZShmaWxlbmFtZSk7XG4gIGlmIChjb25maWcpIHtcbiAgICBjb25maWcgPSBKU09OLnBhcnNlKGNvbmZpZyk7XG4gICAgY29uc29sZS5sb2coXCJCcm93c2VyIENvbmZpZzpcIiwgY29uZmlnKTtcbiAgfSBlbHNlIHtcbiAgICBjb25maWcgPSB7XG4gICAgICBuYXZpZ2F0aW9uX2luc3RydW1lbnQ6dHJ1ZSxcbiAgICAgIGNvb2tpZV9pbnN0cnVtZW50OnRydWUsXG4gICAgICBqc19pbnN0cnVtZW50OnRydWUsXG4gICAgICBjbGVhbmVkX2pzX2luc3RydW1lbnRfc2V0dGluZ3M6XG4gICAgICBbXG4gICAgICAgIHtcbiAgICAgICAgICBvYmplY3Q6IGB3aW5kb3cuQ2FudmFzUmVuZGVyaW5nQ29udGV4dDJELnByb3RvdHlwZWAsXG4gICAgICAgICAgaW5zdHJ1bWVudGVkTmFtZTogXCJDYW52YXNSZW5kZXJpbmdDb250ZXh0MkRcIixcbiAgICAgICAgICBsb2dTZXR0aW5nczoge1xuICAgICAgICAgICAgcHJvcGVydGllc1RvSW5zdHJ1bWVudDogW10sXG4gICAgICAgICAgICBub25FeGlzdGluZ1Byb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgZXhjbHVkZWRQcm9wZXJ0aWVzOiBbXSxcbiAgICAgICAgICAgIGxvZ0NhbGxTdGFjazogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbnNBc1N0cmluZ3M6IGZhbHNlLFxuICAgICAgICAgICAgbG9nRnVuY3Rpb25HZXRzOiBmYWxzZSxcbiAgICAgICAgICAgIHByZXZlbnRTZXRzOiBmYWxzZSxcbiAgICAgICAgICAgIHJlY3Vyc2l2ZTogZmFsc2UsXG4gICAgICAgICAgICBkZXB0aDogNSxcbiAgICAgICAgICB9XG4gICAgICAgIH0sXG4gICAgICBdLFxuICAgICAgaHR0cF9pbnN0cnVtZW50OnRydWUsXG4gICAgICBjYWxsc3RhY2tfaW5zdHJ1bWVudDp0cnVlLFxuICAgICAgc2F2ZV9jb250ZW50OmZhbHNlLFxuICAgICAgdGVzdGluZzp0cnVlLFxuICAgICAgYnJvd3Nlcl9pZDowLFxuICAgICAgY3VzdG9tX3BhcmFtczoge31cbiAgICB9O1xuICAgIGNvbnNvbGUubG9nKFwiV0FSTklORzogY29uZmlnIG5vdCBmb3VuZC4gQXNzdW1pbmcgdGhpcyBpcyBhIHRlc3QgcnVuIG9mXCIsXG4gICAgICAgICAgICAgICAgXCJ0aGUgZXh0ZW5zaW9uLiBPdXRwdXR0aW5nIGFsbCBxdWVyaWVzIHRvIGNvbnNvbGUuXCIsIHtjb25maWd9KTtcbiAgfVxuXG4gIGF3YWl0IGxvZ2dpbmdEQi5vcGVuKGNvbmZpZ1snc3RvcmFnZV9jb250cm9sbGVyX2FkZHJlc3MnXSxcbiAgICAgICAgICAgICAgICAgICAgICAgY29uZmlnWydsb2dnZXJfYWRkcmVzcyddLFxuICAgICAgICAgICAgICAgICAgICAgICBjb25maWdbJ2Jyb3dzZXJfaWQnXSk7XG5cbiAgaWYgKGNvbmZpZ1tcImN1c3RvbV9wYXJhbXNcIl1bXCJwcmVfaW5zdHJ1bWVudGF0aW9uX2NvZGVcIl0pIHtcbiAgICBldmFsKGNvbmZpZ1tcImN1c3RvbV9wYXJhbXNcIl1bXCJwcmVfaW5zdHJ1bWVudGF0aW9uX2NvZGVcIl0pXG4gIH1cbiAgaWYgKGNvbmZpZ1tcIm5hdmlnYXRpb25faW5zdHJ1bWVudFwiXSkge1xuICAgIGxvZ2dpbmdEQi5sb2dEZWJ1ZyhcIk5hdmlnYXRpb24gaW5zdHJ1bWVudGF0aW9uIGVuYWJsZWRcIik7XG4gICAgbGV0IG5hdmlnYXRpb25JbnN0cnVtZW50ID0gbmV3IE5hdmlnYXRpb25JbnN0cnVtZW50KGxvZ2dpbmdEQik7XG4gICAgbmF2aWdhdGlvbkluc3RydW1lbnQucnVuKGNvbmZpZ1tcImJyb3dzZXJfaWRcIl0pO1xuICB9XG5cbiAgaWYgKGNvbmZpZ1snY29va2llX2luc3RydW1lbnQnXSkge1xuICAgIGxvZ2dpbmdEQi5sb2dEZWJ1ZyhcIkNvb2tpZSBpbnN0cnVtZW50YXRpb24gZW5hYmxlZFwiKTtcbiAgICBsZXQgY29va2llSW5zdHJ1bWVudCA9IG5ldyBDb29raWVJbnN0cnVtZW50KGxvZ2dpbmdEQik7XG4gICAgY29va2llSW5zdHJ1bWVudC5ydW4oY29uZmlnWydicm93c2VyX2lkJ10pO1xuICB9XG5cbiAgaWYgKGNvbmZpZ1snanNfaW5zdHJ1bWVudCddKSB7XG4gICAgbG9nZ2luZ0RCLmxvZ0RlYnVnKFwiSmF2YXNjcmlwdCBpbnN0cnVtZW50YXRpb24gZW5hYmxlZFwiKTtcbiAgICBsZXQganNJbnN0cnVtZW50ID0gbmV3IEphdmFzY3JpcHRJbnN0cnVtZW50KGxvZ2dpbmdEQik7XG4gICAganNJbnN0cnVtZW50LnJ1bihjb25maWdbJ2Jyb3dzZXJfaWQnXSk7XG4gICAgYXdhaXQganNJbnN0cnVtZW50LnJlZ2lzdGVyQ29udGVudFNjcmlwdChjb25maWdbJ3Rlc3RpbmcnXSwgY29uZmlnWydjbGVhbmVkX2pzX2luc3RydW1lbnRfc2V0dGluZ3MnXSk7XG4gIH1cblxuICBpZiAoY29uZmlnWydodHRwX2luc3RydW1lbnQnXSkge1xuICAgIGxvZ2dpbmdEQi5sb2dEZWJ1ZyhcIkhUVFAgSW5zdHJ1bWVudGF0aW9uIGVuYWJsZWRcIik7XG4gICAgbGV0IGh0dHBJbnN0cnVtZW50ID0gbmV3IEh0dHBJbnN0cnVtZW50KGxvZ2dpbmdEQik7XG4gICAgaHR0cEluc3RydW1lbnQucnVuKGNvbmZpZ1snYnJvd3Nlcl9pZCddLFxuICAgICAgICAgICAgICAgICAgICAgICBjb25maWdbJ3NhdmVfY29udGVudCddKTtcbiAgfVxuXG4gIGlmIChjb25maWdbJ2NhbGxzdGFja19pbnN0cnVtZW50J10pIHtcbiAgICBsb2dnaW5nREIubG9nRGVidWcoXCJDYWxsc3RhY2sgSW5zdHJ1bWVudGF0aW9uIGVuYWJsZWRcIik7XG4gICAgbGV0IGNhbGxzdGFja0luc3RydW1lbnQgPSBuZXcgQ2FsbHN0YWNrSW5zdHJ1bWVudChsb2dnaW5nREIpO1xuICAgIGNhbGxzdGFja0luc3RydW1lbnQucnVuKGNvbmZpZ1snYnJvd3Nlcl9pZCddKTtcbiAgfVxuICBcbiAgaWYgKGNvbmZpZ1snZG5zX2luc3RydW1lbnQnXSkge1xuICAgIGxvZ2dpbmdEQi5sb2dEZWJ1ZyhcIkROUyBpbnN0cnVtZW50YXRpb24gZW5hYmxlZFwiKTtcbiAgICBsZXQgZG5zSW5zdHJ1bWVudCA9IG5ldyBEbnNJbnN0cnVtZW50KGxvZ2dpbmdEQik7XG4gICAgZG5zSW5zdHJ1bWVudC5ydW4oY29uZmlnWydicm93c2VyX2lkJ10pO1xuICB9XG5cbiAgYXdhaXQgYnJvd3Nlci5wcm9maWxlRGlySU8ud3JpdGVGaWxlKFwiT1BFTldQTV9TVEFSVFVQX1NVQ0NFU1MudHh0XCIsIFwiXCIpO1xufVxuXG5tYWluKCk7XG5cbiJdLCJzb3VyY2VSb290IjoiIn0=