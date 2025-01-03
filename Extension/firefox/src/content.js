/******/ (() => { // webpackBootstrap
/******/ 	"use strict";
/******/ 	var __webpack_modules__ = ({

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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiamF2YXNjcmlwdC1pbnN0cnVtZW50LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vc3JjL2JhY2tncm91bmQvamF2YXNjcmlwdC1pbnN0cnVtZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUNBLE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxNQUFNLHdDQUF3QyxDQUFDO0FBQ2pGLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxNQUFNLCtCQUErQixDQUFDO0FBQ3JFLE9BQU8sRUFBRSxTQUFTLEVBQUUsWUFBWSxFQUFFLFNBQVMsRUFBRSxNQUFNLHFCQUFxQixDQUFDO0FBSXpFLE1BQU0sT0FBTyxvQkFBb0I7SUFDL0I7Ozs7OztPQU1HO0lBQ0ssTUFBTSxDQUFDLHFCQUFxQixDQUFDLElBQUksRUFBRSxNQUFxQjtRQUM5RCxNQUFNLE1BQU0sR0FBRyxFQUF5QixDQUFDO1FBQ3pDLE1BQU0sQ0FBQyxzQkFBc0IsR0FBRyxvQkFBb0IsQ0FBQztRQUNyRCxNQUFNLENBQUMsYUFBYSxHQUFHLHVCQUF1QixFQUFFLENBQUM7UUFDakQsTUFBTSxDQUFDLHlCQUF5QixHQUFHLElBQUksQ0FBQyxPQUFPLENBQUM7UUFDaEQsTUFBTSxDQUFDLFNBQVMsR0FBRyxNQUFNLENBQUMsR0FBRyxDQUFDLFFBQVEsQ0FBQztRQUN2QyxNQUFNLENBQUMsTUFBTSxHQUFHLE1BQU0sQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDO1FBQzlCLE1BQU0sQ0FBQyxRQUFRLEdBQUcsTUFBTSxDQUFDLE9BQU8sQ0FBQztRQUNqQyxNQUFNLENBQUMsVUFBVSxHQUFHLFNBQVMsQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLENBQUM7UUFDOUMsTUFBTSxDQUFDLFdBQVcsR0FBRyxZQUFZLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDO1FBQ25ELE1BQU0sQ0FBQyxVQUFVLEdBQUcsWUFBWSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQztRQUNqRCxNQUFNLENB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

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
  !*** ./content.js/index.js ***!
  \*****************************/
__webpack_require__.r(__webpack_exports__);
/* harmony import */ var openwpm_webext_instrumentation__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! openwpm-webext-instrumentation */ "../webext-instrumentation/build/module/index.js");


(0,openwpm_webext_instrumentation__WEBPACK_IMPORTED_MODULE_0__.injectJavascriptInstrumentPageScript)(window.openWpmContentScriptConfig || {});
delete window.openWpmContentScriptConfig;

})();

/******/ })()
;
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uLi93ZWJleHQtaW5zdHJ1bWVudGF0aW9uL2J1aWxkL21vZHVsZS9iYWNrZ3JvdW5kL2Nvb2tpZS1pbnN0cnVtZW50LmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2JhY2tncm91bmQvZG5zLWluc3RydW1lbnQuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvYmFja2dyb3VuZC9odHRwLWluc3RydW1lbnQuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvYmFja2dyb3VuZC9qYXZhc2NyaXB0LWluc3RydW1lbnQuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvYmFja2dyb3VuZC9uYXZpZ2F0aW9uLWluc3RydW1lbnQuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvY29udGVudC9qYXZhc2NyaXB0LWluc3RydW1lbnQtY29udGVudC1zY29wZS5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uLi93ZWJleHQtaW5zdHJ1bWVudGF0aW9uL2J1aWxkL21vZHVsZS9jb250ZW50L2phdmFzY3JpcHQtaW5zdHJ1bWVudC1wYWdlLXNjb3BlLmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2luZGV4LmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2xpYi9leHRlbnNpb24tc2Vzc2lvbi1ldmVudC1vcmRpbmFsLmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2xpYi9leHRlbnNpb24tc2Vzc2lvbi11dWlkLmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2xpYi9odHRwLXBvc3QtcGFyc2VyLmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2xpYi9qcy1pbnN0cnVtZW50cy5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uLi93ZWJleHQtaW5zdHJ1bWVudGF0aW9uL2J1aWxkL21vZHVsZS9saWIvcGVuZGluZy1uYXZpZ2F0aW9uLmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2xpYi9wZW5kaW5nLXJlcXVlc3QuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvbGliL3BlbmRpbmctcmVzcG9uc2UuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvbGliL3Jlc3BvbnNlLWJvZHktbGlzdGVuZXIuanMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi4vd2ViZXh0LWluc3RydW1lbnRhdGlvbi9idWlsZC9tb2R1bGUvbGliL3NoYTI1Ni5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uLi93ZWJleHQtaW5zdHJ1bWVudGF0aW9uL2J1aWxkL21vZHVsZS9saWIvc3RyaW5nLXV0aWxzLmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL2xpYi91dWlkLmpzIiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4uL3dlYmV4dC1pbnN0cnVtZW50YXRpb24vYnVpbGQvbW9kdWxlL3NjaGVtYS5qcyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC93ZWJwYWNrL2Jvb3RzdHJhcCIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC93ZWJwYWNrL3J1bnRpbWUvZGVmaW5lIHByb3BlcnR5IGdldHRlcnMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvd2VicGFjay9ydW50aW1lL2hhc093blByb3BlcnR5IHNob3J0aGFuZCIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC93ZWJwYWNrL3J1bnRpbWUvbWFrZSBuYW1lc3BhY2Ugb2JqZWN0Iiwid2VicGFjazovL0BvcGVud3BtL3dlYmV4dC1maXJlZm94Ly4vY29udGVudC5qcy9pbmRleC5qcyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFBaUY7QUFDWjtBQUNQO0FBQ3ZEO0FBQ1A7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLDZDQUE2QztBQUM3QztBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSwyREFBMkQ7QUFDM0Q7QUFDQTtBQUNBO0FBQ0Esb0NBQW9DLDREQUFTO0FBQzdDLG9DQUFvQyw0REFBUztBQUM3QyxrQ0FBa0MsNERBQVM7QUFDM0MsNEJBQTRCLCtEQUFZO0FBQ3hDLGlDQUFpQyw0REFBUztBQUMxQyw0QkFBNEIsK0RBQVk7QUFDeEMsNEJBQTRCLCtEQUFZO0FBQ3hDLDZC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QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSx1QkFBdUIscUJBQXFCO0FBQzVDO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxhQUFhO0FBQ2I7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsNENBQTRDLDJCQUEyQixjQUFjLE9BQU8sZ0JBQWdCO0FBQzVHO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLGdEQUFnRCwyQkFBMkIsY0FBYyxPQUFPLGdCQUFnQjtBQUNoSDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLG1DQUFtQztBQUNuQztBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSwrREFBK0Q7QUFDL0Q7QUFDQSwyQ0FBMkM7QUFDM0M7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FB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UDtBQUNBLDJCQUEyQjtBQUMzQjtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDTztBQUNQLDhCQUE4QjtBQUM5QjtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ087QUFDUDtBQUNBO0FBQ0EsMkNBQTJDLHU0RDs7Ozs7Ozs7Ozs7Ozs7QUN0QzNDO0FBQ0E7QUFDQTtBQUNBLGVBQWUsU0FBUztBQUN4QjtBQUNBO0FBQ087QUFDUDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSwyQ0FBMkMsMnpEOzs7Ozs7Ozs7Ozs7OztBQy9CM0M7QUFDTztBQUNQLDJDQUEyQyxtTzs7Ozs7O1VDRjNDO1VBQ0E7O1VBRUE7VUFDQTtVQUNBO1VBQ0E7VUFDQTtVQUNBO1VBQ0E7VUFDQTtVQUNBO1VBQ0E7VUFDQTtVQUNBO1VBQ0E7O1VBRUE7VUFDQTs7VUFFQTtVQUNBO1VBQ0E7Ozs7O1dDdEJBO1dBQ0E7V0FDQTtXQUNBO1dBQ0Esd0NBQXdDLHlDQUF5QztXQUNqRjtXQUNBO1dBQ0EsRTs7Ozs7V0NQQSx3Rjs7Ozs7V0NBQTtXQUNBO1dBQ0E7V0FDQSxzREFBc0Qsa0JBQWtCO1dBQ3hFO1dBQ0EsK0NBQStDLGNBQWM7V0FDN0QsRTs7Ozs7Ozs7Ozs7O0FDTm9GOztBQUVwRixvR0FBb0Msd0NBQXdDO0FBQzVFIiwiZmlsZSI6ImNvbnRlbnQuanMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBpbmNyZW1lbnRlZEV2ZW50T3JkaW5hbCB9IGZyb20gXCIuLi9saWIvZXh0ZW5zaW9uLXNlc3Npb24tZXZlbnQtb3JkaW5hbFwiO1xuaW1wb3J0IHsgZXh0ZW5zaW9uU2Vzc2lvblV1aWQgfSBmcm9tIFwiLi4vbGliL2V4dGVuc2lvbi1zZXNzaW9uLXV1aWRcIjtcbmltcG9ydCB7IGJvb2xUb0ludCwgZXNjYXBlU3RyaW5nIH0gZnJvbSBcIi4uL2xpYi9zdHJpbmctdXRpbHNcIjtcbmV4cG9ydCBjb25zdCB0cmFuc2Zvcm1Db29raWVPYmplY3RUb01hdGNoT3BlbldQTVNjaGVtYSA9IChjb29raWUpID0+IHtcbiAgICBjb25zdCBqYXZhc2NyaXB0Q29va2llID0ge307XG4gICAgLy8gRXhwaXJ5IHRpbWUgKGluIHNlY29uZHMpXG4gICAgLy8gTWF5IHJldHVybiB+TWF4KGludDY0KS4gSSBiZWxpZXZlIHRoaXMgaXMgYSBzZXNzaW9uXG4gICAgLy8gY29va2llIHdoaWNoIGRvZXNuJ3QgZXhwaXJlLiBTZXNzaW9ucyBjb29raWVzIHdpdGhcbiAgICAvLyBub24tbWF4IGV4cGlyeSB0aW1lIGV4cGlyZSBhZnRlciBzZXNzaW9uIG9yIGF0IGV4cGlyeS5cbiAgICBjb25zdCBleHBpcnlUaW1lID0gY29va2llLmV4cGlyYXRpb25EYXRlOyAvLyByZXR1cm5zIHNlY29uZHNcbiAgICBsZXQgZXhwaXJ5VGltZVN0cmluZztcbiAgICBjb25zdCBtYXhJbnQ2NCA9IDkyMjMzNzIwMzY4NTQ3NzYwMDA7XG4gICAgaWYgKCFjb29raWUuZXhwaXJhdGlvbkRhdGUgfHwgZXhwaXJ5VGltZSA9PT0gbWF4SW50NjQpIHtcbiAgICAgICAgZXhwaXJ5VGltZVN0cmluZyA9IFwiOTk5OS0xMi0zMVQyMTo1OTo1OS4wMDBaXCI7XG4gICAgfVxuICAgIGVsc2Uge1xuICAgICAgICBjb25zdCBleHBpcnlUaW1lRGF0ZSA9IG5ldyBEYXRlKGV4cGlyeVRpbWUgKiAxMDAwKTsgLy8gcmVxdWlyZXMgbWlsbGlzZWNvbmRzXG4gICAgICAgIGV4cGlyeVRpbWVTdHJpbmcgPSBleHBpcnlUaW1lRGF0ZS50b0lTT1N0cmluZygpO1xuICAgIH1cbiAgICBqYXZhc2NyaXB0Q29va2llLmV4cGlyeSA9IGV4cGlyeVRpbWVTdHJpbmc7XG4gICAgamF2YXNjcmlwdENvb2tpZS5pc19odHRwX29ubHkgPSBib29sVG9JbnQoY29va2llLmh0dHBPbmx5KTtcbiAgICBqYXZhc2NyaXB0Q29va2llLmlzX2hvc3Rfb25seSA9IGJvb2xUb0ludChjb29raWUuaG9zdE9ubHkpO1xuICAgIGphdmFzY3JpcHRDb29raWUuaXNfc2Vzc2lvbiA9IGJvb2xUb0ludChjb29raWUuc2Vzc2lvbik7XG4gICAgamF2YXNjcmlwdENvb2tpZS5ob3N0ID0gZXNjYXBlU3RyaW5nKGNvb2tpZS5kb21haW4pO1xuICAgIGphdmFzY3JpcHRDb29raWUuaXNfc2VjdXJlID0gYm9vbFRvSW50KGNvb2tpZS5zZWN1cmUpO1xuICAgIGphdmFzY3JpcHRDb29raWUubmFtZSA9IGVzY2FwZVN0cmluZyhjb29raWUubmFtZSk7XG4gICAgamF2YXNjcmlwdENvb2tpZS5wYXRoID0gZXNjYXBlU3RyaW5nKGNvb2tpZS5wYXRoKTtcbiAgICBqYXZhc2NyaXB0Q29va2llLnZhbHVlID0gZXNjYXBlU3RyaW5nKGNvb2tpZS52YWx1ZSk7XG4gICAgamF2YXNjcmlwdENvb2tpZS5zYW1lX3NpdGUgPSBlc2NhcGVTdHJpbmcoY29va2llLnNhbWVTaXRlKTtcbiAgICBqYXZhc2NyaXB0Q29va2llLmZpcnN0X3BhcnR5X2RvbWFpbiA9IGVzY2FwZVN0cmluZyhjb29raWUuZmlyc3RQYXJ0eURvbWFpbik7XG4gICAgamF2YXNjcmlwdENvb2tpZS5zdG9yZV9pZCA9IGVzY2FwZVN0cmluZyhjb29raWUuc3RvcmVJZCk7XG4gICAgamF2YXNjcmlwdENvb2tpZS50aW1lX3N0YW1wID0gbmV3IERhdGUoKS50b0lTT1N0cmluZygpO1xuICAgIHJldHVybiBqYXZhc2NyaXB0Q29va2llO1xufTtcbmV4cG9ydCBjbGFzcyBDb29raWVJbnN0cnVtZW50IHtcbiAgICBkYXRhUmVjZWl2ZXI7XG4gICAgb25DaGFuZ2VkTGlzdGVuZXI7XG4gICAgY29uc3RydWN0b3IoZGF0YVJlY2VpdmVyKSB7XG4gICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyID0gZGF0YVJlY2VpdmVyO1xuICAgIH1cbiAgICBydW4oY3Jhd2xJRCkge1xuICAgICAgICAvLyBJbnN0cnVtZW50IGNvb2tpZSBjaGFuZ2VzXG4gICAgICAgIHRoaXMub25DaGFuZ2VkTGlzdGVuZXIgPSBhc3luYyAoY2hhbmdlSW5mbykgPT4ge1xuICAgICAgICAgICAgY29uc3QgZXZlbnRUeXBlID0gY2hhbmdlSW5mby5yZW1vdmVkID8gXCJkZWxldGVkXCIgOiBcImFkZGVkLW9yLWNoYW5nZWRcIjtcbiAgICAgICAgICAgIGNvbnN0IHVwZGF0ZSA9IHtcbiAgICAgICAgICAgICAgICByZWNvcmRfdHlwZTogZXZlbnRUeXBlLFxuICAgICAgICAgICAgICAgIGNoYW5nZV9jYXVzZTogY2hhbmdlSW5mby5jYXVzZSxcbiAgICAgICAgICAgICAgICBicm93c2VyX2lkOiBjcmF3bElELFxuICAgICAgICAgICAgICAgIGV4dGVuc2lvbl9zZXNzaW9uX3V1aWQ6IGV4dGVuc2lvblNlc3Npb25VdWlkLFxuICAgICAgICAgICAgICAgIGV2ZW50X29yZGluYWw6IGluY3JlbWVudGVkRXZlbnRPcmRpbmFsKCksXG4gICAgICAgICAgICAgICAgLi4udHJhbnNmb3JtQ29va2llT2JqZWN0VG9NYXRjaE9wZW5XUE1TY2hlbWEoY2hhbmdlSW5mby5jb29raWUpLFxuICAgICAgICAgICAgfTtcbiAgICAgICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLnNhdmVSZWNvcmQoXCJqYXZhc2NyaXB0X2Nvb2tpZXNcIiwgdXBkYXRlKTtcbiAgICAgICAgfTtcbiAgICAgICAgYnJvd3Nlci5jb29raWVzLm9uQ2hhbmdlZC5hZGRMaXN0ZW5lcih0aGlzLm9uQ2hhbmdlZExpc3RlbmVyKTtcbiAgICB9XG4gICAgYXN5bmMgc2F2ZUFsbENvb2tpZXMoY3Jhd2xJRCkge1xuICAgICAgICBjb25zdCBhbGxDb29raWVzID0gYXdhaXQgYnJvd3Nlci5jb29raWVzLmdldEFsbCh7fSk7XG4gICAgICAgIGF3YWl0IFByb21pc2UuYWxsKGFsbENvb2tpZXMubWFwKChjb29raWUpID0+IHtcbiAgICAgICAgICAgIGNvbnN0IHVwZGF0ZSA9IHtcbiAgICAgICAgICAgICAgICByZWNvcmRfdHlwZTogXCJtYW51YWwtZXhwb3J0XCIsXG4gICAgICAgICAgICAgICAgYnJvd3Nlcl9pZDogY3Jhd2xJRCxcbiAgICAgICAgICAgICAgICBleHRlbnNpb25fc2Vzc2lvbl91dWlkOiBleHRlbnNpb25TZXNzaW9uVXVpZCxcbiAgICAgICAgICAgICAgICAuLi50cmFuc2Zvcm1Db29raWVPYmplY3RUb01hdGNoT3BlbldQTVNjaGVtYShjb29raWUpLFxuICAgICAgICAgICAgfTtcbiAgICAgICAgICAgIHJldHVybiB0aGlzLmRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwiamF2YXNjcmlwdF9jb29raWVzXCIsIHVwZGF0ZSk7XG4gICAgICAgIH0pKTtcbiAgICB9XG4gICAgY2xlYW51cCgpIHtcbiAgICAgICAgaWYgKHRoaXMub25DaGFuZ2VkTGlzdGVuZXIpIHtcbiAgICAgICAgICAgIGJyb3dzZXIuY29va2llcy5vbkNoYW5nZWQucmVtb3ZlTGlzdGVuZXIodGhpcy5vbkNoYW5nZWRMaXN0ZW5lcik7XG4gICAgICAgIH1cbiAgICB9XG59XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2lZMjl2YTJsbExXbHVjM1J5ZFcxbGJuUXVhbk1pTENKemIzVnlZMlZTYjI5MElqb2lJaXdpYzI5MWNtTmxjeUk2V3lJdUxpOHVMaTh1TGk5emNtTXZZbUZqYTJkeWIzVnVaQzlqYjI5cmFXVXRhVzV6ZEhKMWJXVnVkQzUwY3lKZExDSnVZVzFsY3lJNlcxMHNJbTFoY0hCcGJtZHpJam9pUVVGQlFTeFBRVUZQTEVWQlFVVXNkVUpCUVhWQ0xFVkJRVVVzVFVGQlRTeDNRMEZCZDBNc1EwRkJRenRCUVVOcVJpeFBRVUZQTEVWQlFVVXNiMEpCUVc5Q0xFVkJRVVVzVFVGQlRTd3JRa0ZCSzBJc1EwRkJRenRCUVVOeVJTeFBRVUZQTEVWQlFVVXNVMEZCVXl4RlFVRkZMRmxCUVZrc1JVRkJSU3hOUVVGTkxIRkNRVUZ4UWl4RFFVRkRPMEZCU3psRUxFMUJRVTBzUTBGQlF5eE5RVUZOTEhsRFFVRjVReXhIUVVGSExFTkJRVU1zVFVGQll5eEZRVUZGTEVWQlFVVTdTVUZETVVVc1RVRkJUU3huUWtGQlowSXNSMEZCUnl4RlFVRnpRaXhEUVVGRE8wbEJSV2hFTERKQ1FVRXlRanRKUVVNelFpeHpSRUZCYzBRN1NVRkRkRVFzY1VSQlFYRkVPMGxCUTNKRUxIbEVRVUY1UkR0SlFVTjZSQ3hOUVVGTkxGVkJRVlVzUjBGQlJ5eE5RVUZOTEVOQlFVTXNZMEZCWXl4RFFVRkRMRU5CUVVNc2EwSkJRV3RDTzBsQlF6VkVMRWxCUVVrc1owSkJRV2RDTEVOQlFVTTdTVUZEY2tJc1RVRkJUU3hSUVVGUkxFZEJRVWNzYlVKQlFXMUNMRU5CUVVNN1NVRkRja01zU1VGQlNTeERRVUZETEUxQlFVMHNRMEZCUXl4alFVRmpMRWxCUVVrc1ZVRkJWU3hMUVVGTExGRkJRVkVzUlVGQlJUdFJRVU55UkN4blFrRkJaMElzUjBGQlJ5d3dRa0ZCTUVJc1EwRkJRenRMUVVNdlF6dFRRVUZOTzFGQlEwd3NUVUZCVFN4alFVRmpMRWRCUVVjc1NVRkJTU3hKUVVGSkxFTkJRVU1zVlVGQlZTeEhRVUZITEVsQlFVa3NRMEZCUXl4RFFVRkRMRU5CUVVNc2QwSkJRWGRDTzFGQlF6VkZMR2RDUVVGblFpeEhRVUZITEdOQlFXTXNRMEZCUXl4WFFVRlhMRVZCUVVVc1EwRkJRenRMUVVOcVJEdEpRVU5FTEdkQ1FVRm5RaXhEUVVGRExFMUJRVTBzUjBGQlJ5eG5Ra0ZCWjBJc1EwRkJRenRKUVVNelF5eG5Ra0ZCWjBJc1EwRkJReXhaUVVGWkxFZEJRVWNzVTBGQlV5eERRVUZETEUxQlFVMHNRMEZCUXl4UlFVRlJMRU5CUVVNc1EwRkJRenRKUVVNelJDeG5Ra0ZCWjBJc1EwRkJReXhaUVVGWkxFZEJRVWNzVTBGQlV5eERRVUZETEUxQlFVMHNRMEZCUXl4UlFVRlJMRU5CUVVNc1EwRkJRenRKUVVNelJDeG5Ra0ZCWjBJc1EwRkJReXhWUVVGVkxFZEJRVWNzVTBGQlV5eERRVUZETEUxQlFVMHNRMEZCUXl4UFFVRlBMRU5CUVVNc1EwRkJRenRKUVVWNFJDeG5Ra0ZCWjBJc1EwRkJReXhKUVVGSkxFZEJRVWNzV1VGQldTeERRVUZETEUxQlFVMHNRMEZCUXl4TlFVRk5MRU5CUVVNc1EwRkJRenRKUVVOd1JDeG5Ra0ZCWjBJc1EwRkJReXhUUVVGVExFZEJRVWNzVTBGQlV5eERRVUZETEUxQlFVMHNRMEZCUXl4TlFVRk5MRU5CUVVNc1EwRkJRenRKUVVOMFJDeG5Ra0ZCWjBJc1EwRkJReXhKUVVGSkxFZEJRVWNzV1VGQldTeERRVUZETEUxQlFVMHNRMEZCUXl4SlFVRkpMRU5CUVVNc1EwRkJRenRKUVVOc1JDeG5Ra0ZCWjBJc1EwRkJReXhKUVVGSkxFZEJRVWNzV1VGQldTeERRVUZETEUxQlFVMHNRMEZCUXl4SlFVRkpMRU5CUVVNc1EwRkJRenRKUVVOc1JDeG5Ra0ZCWjBJc1EwRkJReXhMUVVGTExFZEJRVWNzV1VGQldTeERRVUZETEUxQlFVMHNRMEZCUXl4TFFVRkxMRU5CUVVNc1EwRkJRenRKUVVOd1JDeG5Ra0ZCWjBJc1EwRkJReXhUUVVGVExFZEJRVWNzV1VGQldTeERRVUZETEUxQlFVMHNRMEZCUXl4UlFVRlJMRU5CUVVNc1EwRkJRenRKUVVNelJDeG5Ra0ZCWjBJc1EwRkJReXhyUWtGQmEwSXNSMEZCUnl4WlFVRlpMRU5CUVVNc1RVRkJUU3hEUVVGRExHZENRVUZuUWl4RFFVRkRMRU5CUVVNN1NVRkROVVVzWjBKQlFXZENMRU5CUVVNc1VVRkJVU3hIUVVGSExGbEJRVmtzUTBGQlF5eE5RVUZOTEVOQlFVTXNUMEZCVHl4RFFVRkRMRU5CUVVNN1NVRkZla1FzWjBKQlFXZENMRU5CUVVNc1ZVRkJWU3hIUVVGSExFbEJRVWtzU1VGQlNTeEZRVUZGTEVOQlFVTXNWMEZCVnl4RlFVRkZMRU5CUVVNN1NVRkZka1FzVDBGQlR5eG5Ra0ZCWjBJc1EwRkJRenRCUVVNeFFpeERRVUZETEVOQlFVTTdRVUZGUml4TlFVRk5MRTlCUVU4c1owSkJRV2RDTzBsQlExWXNXVUZCV1N4RFFVRkRPMGxCUTNSQ0xHbENRVUZwUWl4RFFVRkRPMGxCUlRGQ0xGbEJRVmtzV1VGQldUdFJRVU4wUWl4SlFVRkpMRU5CUVVNc1dVRkJXU3hIUVVGSExGbEJRVmtzUTBGQlF6dEpRVU51UXl4RFFVRkRPMGxCUlUwc1IwRkJSeXhEUVVGRExFOUJRVTg3VVVGRGFFSXNORUpCUVRSQ08xRkJRelZDTEVsQlFVa3NRMEZCUXl4cFFrRkJhVUlzUjBGQlJ5eExRVUZMTEVWQlFVVXNWVUZQTDBJc1JVRkJSU3hGUVVGRk8xbEJRMGdzVFVGQlRTeFRRVUZUTEVkQlFVY3NWVUZCVlN4RFFVRkRMRTlCUVU4c1EwRkJReXhEUVVGRExFTkJRVU1zVTBGQlV5eERRVUZETEVOQlFVTXNRMEZCUXl4clFrRkJhMElzUTBGQlF6dFpRVU4wUlN4TlFVRk5MRTFCUVUwc1IwRkJNa0k3WjBKQlEzSkRMRmRCUVZjc1JVRkJSU3hUUVVGVE8yZENRVU4wUWl4WlFVRlpMRVZCUVVVc1ZVRkJWU3hEUVVGRExFdEJRVXM3WjBKQlF6bENMRlZCUVZVc1JVRkJSU3hQUVVGUE8yZENRVU51UWl4elFrRkJjMElzUlVGQlJTeHZRa0ZCYjBJN1owSkJRelZETEdGQlFXRXNSVUZCUlN4MVFrRkJkVUlzUlVGQlJUdG5Ra0ZEZUVNc1IwRkJSeXg1UTBGQmVVTXNRMEZCUXl4VlFVRlZMRU5CUVVNc1RVRkJUU3hEUVVGRE8yRkJRMmhGTEVOQlFVTTdXVUZEUml4SlFVRkpMRU5CUVVNc1dVRkJXU3hEUVVGRExGVkJRVlVzUTBGQlF5eHZRa0ZCYjBJc1JVRkJSU3hOUVVGTkxFTkJRVU1zUTBGQlF6dFJRVU0zUkN4RFFVRkRMRU5CUVVNN1VVRkRSaXhQUVVGUExFTkJRVU1zVDBGQlR5eERRVUZETEZOQlFWTXNRMEZCUXl4WFFVRlhMRU5CUVVNc1NVRkJTU3hEUVVGRExHbENRVUZwUWl4RFFVRkRMRU5CUVVNN1NVRkRhRVVzUTBGQlF6dEpRVVZOTEV0QlFVc3NRMEZCUXl4alFVRmpMRU5CUVVNc1QwRkJUenRSUVVOcVF5eE5RVUZOTEZWQlFWVXNSMEZCUnl4TlFVRk5MRTlCUVU4c1EwRkJReXhQUVVGUExFTkJRVU1zVFVGQlRTeERRVUZETEVWQlFVVXNRMEZCUXl4RFFVRkRPMUZCUTNCRUxFMUJRVTBzVDBGQlR5eERRVUZETEVkQlFVY3NRMEZEWml4VlFVRlZMRU5CUVVNc1IwRkJSeXhEUVVGRExFTkJRVU1zVFVGQll5eEZRVUZGTEVWQlFVVTdXVUZEYUVNc1RVRkJUU3hOUVVGTkxFZEJRVEpDTzJkQ1FVTnlReXhYUVVGWExFVkJRVVVzWlVGQlpUdG5Ra0ZETlVJc1ZVRkJWU3hGUVVGRkxFOUJRVTg3WjBKQlEyNUNMSE5DUVVGelFpeEZRVUZGTEc5Q1FVRnZRanRuUWtGRE5VTXNSMEZCUnl4NVEwRkJlVU1zUTBGQlF5eE5RVUZOTEVOQlFVTTdZVUZEY2tRc1EwRkJRenRaUVVOR0xFOUJRVThzU1VGQlNTeERRVUZETEZsQlFWa3NRMEZCUXl4VlFVRlZMRU5CUVVNc2IwSkJRVzlDTEVWQlFVVXNUVUZCVFN4RFFVRkRMRU5CUVVNN1VVRkRjRVVzUTBGQlF5eERRVUZETEVOQlEwZ3NRMEZCUXp0SlFVTktMRU5CUVVNN1NVRkZUU3hQUVVGUE8xRkJRMW9zU1VGQlNTeEpRVUZKTEVOQlFVTXNhVUpCUVdsQ0xFVkJRVVU3V1VGRE1VSXNUMEZCVHl4RFFVRkRMRTlCUVU4c1EwRkJReXhUUVVGVExFTkJRVU1zWTBGQll5eERRVUZETEVsQlFVa3NRMEZCUXl4cFFrRkJhVUlzUTBGQlF5eERRVUZETzFOQlEyeEZPMGxCUTBnc1EwRkJRenREUVVOR0luMD0iLCJpbXBvcnQgeyBQZW5kaW5nUmVzcG9uc2UgfSBmcm9tIFwiLi4vbGliL3BlbmRpbmctcmVzcG9uc2VcIjtcbmltcG9ydCB7IGFsbFR5cGVzIH0gZnJvbSBcIi4vaHR0cC1pbnN0cnVtZW50XCI7XG5leHBvcnQgY2xhc3MgRG5zSW5zdHJ1bWVudCB7XG4gICAgZGF0YVJlY2VpdmVyO1xuICAgIG9uQ29tcGxldGVMaXN0ZW5lcjtcbiAgICBwZW5kaW5nUmVzcG9uc2VzID0ge307XG4gICAgY29uc3RydWN0b3IoZGF0YVJlY2VpdmVyKSB7XG4gICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyID0gZGF0YVJlY2VpdmVyO1xuICAgIH1cbiAgICBydW4oY3Jhd2xJRCkge1xuICAgICAgICBjb25zdCBmaWx0ZXIgPSB7IHVybHM6IFtcIjxhbGxfdXJscz5cIl0sIHR5cGVzOiBhbGxUeXBlcyB9O1xuICAgICAgICBjb25zdCByZXF1ZXN0U3RlbXNGcm9tRXh0ZW5zaW9uID0gKGRldGFpbHMpID0+IHtcbiAgICAgICAgICAgIHJldHVybiAoZGV0YWlscy5vcmlnaW5VcmwgJiZcbiAgICAgICAgICAgICAgICBkZXRhaWxzLm9yaWdpblVybC5pbmRleE9mKFwibW96LWV4dGVuc2lvbjovL1wiKSA+IC0xICYmXG4gICAgICAgICAgICAgICAgZGV0YWlscy5vcmlnaW5VcmwuaW5jbHVkZXMoXCJmYWtlUmVxdWVzdFwiKSk7XG4gICAgICAgIH07XG4gICAgICAgIC8qXG4gICAgICAgICAqIEF0dGFjaCBoYW5kbGVycyB0byBldmVudCBsaXN0ZW5lcnNcbiAgICAgICAgICovXG4gICAgICAgIHRoaXMub25Db21wbGV0ZUxpc3RlbmVyID0gKGRldGFpbHMpID0+IHtcbiAgICAgICAgICAgIC8vIElnbm9yZSByZXF1ZXN0cyBtYWRlIGJ5IGV4dGVuc2lvbnNcbiAgICAgICAgICAgIGlmIChyZXF1ZXN0U3RlbXNGcm9tRXh0ZW5zaW9uKGRldGFpbHMpKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgY29uc3QgcGVuZGluZ1Jlc3BvbnNlID0gdGhpcy5nZXRQZW5kaW5nUmVzcG9uc2UoZGV0YWlscy5yZXF1ZXN0SWQpO1xuICAgICAgICAgICAgcGVuZGluZ1Jlc3BvbnNlLnJlc29sdmVPbkNvbXBsZXRlZEV2ZW50RGV0YWlscyhkZXRhaWxzKTtcbiAgICAgICAgICAgIHRoaXMub25Db21wbGV0ZURuc0hhbmRsZXIoZGV0YWlscywgY3Jhd2xJRCk7XG4gICAgICAgIH07XG4gICAgICAgIGJyb3dzZXIud2ViUmVxdWVzdC5vbkNvbXBsZXRlZC5hZGRMaXN0ZW5lcih0aGlzLm9uQ29tcGxldGVMaXN0ZW5lciwgZmlsdGVyKTtcbiAgICB9XG4gICAgY2xlYW51cCgpIHtcbiAgICAgICAgaWYgKHRoaXMub25Db21wbGV0ZUxpc3RlbmVyKSB7XG4gICAgICAgICAgICBicm93c2VyLndlYlJlcXVlc3Qub25Db21wbGV0ZWQucmVtb3ZlTGlzdGVuZXIodGhpcy5vbkNvbXBsZXRlTGlzdGVuZXIpO1xuICAgICAgICB9XG4gICAgfVxuICAgIGdldFBlbmRpbmdSZXNwb25zZShyZXF1ZXN0SWQpIHtcbiAgICAgICAgaWYgKCF0aGlzLnBlbmRpbmdSZXNwb25zZXNbcmVxdWVzdElkXSkge1xuICAgICAgICAgICAgdGhpcy5wZW5kaW5nUmVzcG9uc2VzW3JlcXVlc3RJZF0gPSBuZXcgUGVuZGluZ1Jlc3BvbnNlKCk7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHRoaXMucGVuZGluZ1Jlc3BvbnNlc1tyZXF1ZXN0SWRdO1xuICAgIH1cbiAgICBoYW5kbGVSZXNvbHZlZERuc0RhdGEoZG5zUmVjb3JkT2JqLCBkYXRhUmVjZWl2ZXIpIHtcbiAgICAgICAgLy8gQ3VycmluZyB0aGUgZGF0YSByZXR1cm5lZCBieSBBUEkgY2FsbC5cbiAgICAgICAgcmV0dXJuIGZ1bmN0aW9uIChyZWNvcmQpIHtcbiAgICAgICAgICAgIC8vIEdldCBkYXRhIGZyb20gQVBJIGNhbGxcbiAgICAgICAgICAgIGRuc1JlY29yZE9iai5hZGRyZXNzZXMgPSByZWNvcmQuYWRkcmVzc2VzLnRvU3RyaW5nKCk7XG4gICAgICAgICAgICBkbnNSZWNvcmRPYmouY2Fub25pY2FsX25hbWUgPSByZWNvcmQuY2Fub25pY2FsTmFtZTtcbiAgICAgICAgICAgIGRuc1JlY29yZE9iai5pc19UUlIgPSByZWNvcmQuaXNUUlI7XG4gICAgICAgICAgICAvLyBTZW5kIGRhdGEgdG8gbWFpbiBPcGVuV1BNIGRhdGEgYWdncmVnYXRvci5cbiAgICAgICAgICAgIGRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwiZG5zX3Jlc3BvbnNlc1wiLCBkbnNSZWNvcmRPYmopO1xuICAgICAgICB9O1xuICAgIH1cbiAgICBhc3luYyBvbkNvbXBsZXRlRG5zSGFuZGxlcihkZXRhaWxzLCBjcmF3bElEKSB7XG4gICAgICAgIC8vIENyZWF0ZSBhbmQgcG9wdWxhdGUgRG5zUmVzb2x2ZSBvYmplY3RcbiAgICAgICAgY29uc3QgZG5zUmVjb3JkID0ge307XG4gICAgICAgIGRuc1JlY29yZC5icm93c2VyX2lkID0gY3Jhd2xJRDtcbiAgICAgICAgZG5zUmVjb3JkLnJlcXVlc3RfaWQgPSBOdW1iZXIoZGV0YWlscy5yZXF1ZXN0SWQpO1xuICAgICAgICBkbnNSZWNvcmQudXNlZF9hZGRyZXNzID0gZGV0YWlscy5pcDtcbiAgICAgICAgY29uc3QgY3VycmVudFRpbWUgPSBuZXcgRGF0ZShkZXRhaWxzLnRpbWVTdGFtcCk7XG4gICAgICAgIGRuc1JlY29yZC50aW1lX3N0YW1wID0gY3VycmVudFRpbWUudG9JU09TdHJpbmcoKTtcbiAgICAgICAgLy8gUXVlcnkgRE5TIEFQSVxuICAgICAgICBjb25zdCB1cmwgPSBuZXcgVVJMKGRldGFpbHMudXJsKTtcbiAgICAgICAgZG5zUmVjb3JkLmhvc3RuYW1lID0gdXJsLmhvc3RuYW1lO1xuICAgICAgICBjb25zdCBkbnNSZXNvbHZlID0gYnJvd3Nlci5kbnMucmVzb2x2ZShkbnNSZWNvcmQuaG9zdG5hbWUsIFtcbiAgICAgICAgICAgIFwiY2Fub25pY2FsX25hbWVcIixcbiAgICAgICAgXSk7XG4gICAgICAgIGRuc1Jlc29sdmUudGhlbih0aGlzLmhhbmRsZVJlc29sdmVkRG5zRGF0YShkbnNSZWNvcmQsIHRoaXMuZGF0YVJlY2VpdmVyKSk7XG4gICAgfVxufVxuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pWkc1ekxXbHVjM1J5ZFcxbGJuUXVhbk1pTENKemIzVnlZMlZTYjI5MElqb2lJaXdpYzI5MWNtTmxjeUk2V3lJdUxpOHVMaTh1TGk5emNtTXZZbUZqYTJkeWIzVnVaQzlrYm5NdGFXNXpkSEoxYldWdWRDNTBjeUpkTENKdVlXMWxjeUk2VzEwc0ltMWhjSEJwYm1keklqb2lRVUZCUVN4UFFVRlBMRVZCUVVVc1pVRkJaU3hGUVVGRkxFMUJRVTBzZVVKQlFYbENMRU5CUVVNN1FVRkhNVVFzVDBGQlR5eEZRVUZGTEZGQlFWRXNSVUZCUlN4TlFVRk5MRzFDUVVGdFFpeERRVUZETzBGQlJ6ZERMRTFCUVUwc1QwRkJUeXhoUVVGaE8wbEJRMUFzV1VGQldTeERRVUZETzBsQlEzUkNMR3RDUVVGclFpeERRVUZETzBsQlEyNUNMR2RDUVVGblFpeEhRVVZ3UWl4RlFVRkZMRU5CUVVNN1NVRkZVQ3haUVVGWkxGbEJRVms3VVVGRGRFSXNTVUZCU1N4RFFVRkRMRmxCUVZrc1IwRkJSeXhaUVVGWkxFTkJRVU03U1VGRGJrTXNRMEZCUXp0SlFVVk5MRWRCUVVjc1EwRkJReXhQUVVGUE8xRkJRMmhDTEUxQlFVMHNUVUZCVFN4SFFVRnJRaXhGUVVGRkxFbEJRVWtzUlVGQlJTeERRVUZETEZsQlFWa3NRMEZCUXl4RlFVRkZMRXRCUVVzc1JVRkJSU3hSUVVGUkxFVkJRVVVzUTBGQlF6dFJRVVY0UlN4TlFVRk5MSGxDUVVGNVFpeEhRVUZITEVOQlFVTXNUMEZCVHl4RlFVRkZMRVZCUVVVN1dVRkROVU1zVDBGQlR5eERRVU5NTEU5QlFVOHNRMEZCUXl4VFFVRlRPMmRDUVVOcVFpeFBRVUZQTEVOQlFVTXNVMEZCVXl4RFFVRkRMRTlCUVU4c1EwRkJReXhyUWtGQmEwSXNRMEZCUXl4SFFVRkhMRU5CUVVNc1EwRkJRenRuUWtGRGJFUXNUMEZCVHl4RFFVRkRMRk5CUVZNc1EwRkJReXhSUVVGUkxFTkJRVU1zWVVGQllTeERRVUZETEVOQlF6RkRMRU5CUVVNN1VVRkRTaXhEUVVGRExFTkJRVU03VVVGRlJqczdWMEZGUnp0UlFVTklMRWxCUVVrc1EwRkJReXhyUWtGQmEwSXNSMEZCUnl4RFFVRkRMRTlCUVRCRExFVkJRVVVzUlVGQlJUdFpRVU4yUlN4eFEwRkJjVU03V1VGRGNrTXNTVUZCU1N4NVFrRkJlVUlzUTBGQlF5eFBRVUZQTEVOQlFVTXNSVUZCUlR0blFrRkRkRU1zVDBGQlR6dGhRVU5TTzFsQlEwUXNUVUZCVFN4bFFVRmxMRWRCUVVjc1NVRkJTU3hEUVVGRExHdENRVUZyUWl4RFFVRkRMRTlCUVU4c1EwRkJReXhUUVVGVExFTkJRVU1zUTBGQlF6dFpRVU51UlN4bFFVRmxMRU5CUVVNc09FSkJRVGhDTEVOQlFVTXNUMEZCVHl4RFFVRkRMRU5CUVVNN1dVRkZlRVFzU1VGQlNTeERRVUZETEc5Q1FVRnZRaXhEUVVGRExFOUJRVThzUlVGQlJTeFBRVUZQTEVOQlFVTXNRMEZCUXp0UlFVTTVReXhEUVVGRExFTkJRVU03VVVGRlJpeFBRVUZQTEVOQlFVTXNWVUZCVlN4RFFVRkRMRmRCUVZjc1EwRkJReXhYUVVGWExFTkJRVU1zU1VGQlNTeERRVUZETEd0Q1FVRnJRaXhGUVVGRkxFMUJRVTBzUTBGQlF5eERRVUZETzBsQlF6bEZMRU5CUVVNN1NVRkZUU3hQUVVGUE8xRkJRMW9zU1VGQlNTeEpRVUZKTEVOQlFVTXNhMEpCUVd0Q0xFVkJRVVU3V1VGRE0wSXNUMEZCVHl4RFFVRkRMRlZCUVZVc1EwRkJReXhYUVVGWExFTkJRVU1zWTBGQll5eERRVUZETEVsQlFVa3NRMEZCUXl4clFrRkJhMElzUTBGQlF5eERRVUZETzFOQlEzaEZPMGxCUTBnc1EwRkJRenRKUVVWUExHdENRVUZyUWl4RFFVRkRMRk5CUVZNN1VVRkRiRU1zU1VGQlNTeERRVUZETEVsQlFVa3NRMEZCUXl4blFrRkJaMElzUTBGQlF5eFRRVUZUTEVOQlFVTXNSVUZCUlR0WlFVTnlReXhKUVVGSkxFTkJRVU1zWjBKQlFXZENMRU5CUVVNc1UwRkJVeXhEUVVGRExFZEJRVWNzU1VGQlNTeGxRVUZsTEVWQlFVVXNRMEZCUXp0VFFVTXhSRHRSUVVORUxFOUJRVThzU1VGQlNTeERRVUZETEdkQ1FVRm5RaXhEUVVGRExGTkJRVk1zUTBGQlF5eERRVUZETzBsQlF6RkRMRU5CUVVNN1NVRkZUeXh4UWtGQmNVSXNRMEZCUXl4WlFVRlpMRVZCUVVVc1dVRkJXVHRSUVVOMFJDeDVRMEZCZVVNN1VVRkRla01zVDBGQlR5eFZRVUZWTEUxQlFVMDdXVUZEY2tJc2VVSkJRWGxDTzFsQlEzcENMRmxCUVZrc1EwRkJReXhUUVVGVExFZEJRVWNzVFVGQlRTeERRVUZETEZOQlFWTXNRMEZCUXl4UlFVRlJMRVZCUVVVc1EwRkJRenRaUVVOeVJDeFpRVUZaTEVOQlFVTXNZMEZCWXl4SFFVRkhMRTFCUVUwc1EwRkJReXhoUVVGaExFTkJRVU03V1VGRGJrUXNXVUZCV1N4RFFVRkRMRTFCUVUwc1IwRkJSeXhOUVVGTkxFTkJRVU1zUzBGQlN5eERRVUZETzFsQlJXNURMRFpEUVVFMlF6dFpRVU0zUXl4WlFVRlpMRU5CUVVNc1ZVRkJWU3hEUVVGRExHVkJRV1VzUlVGQlJTeFpRVUZaTEVOQlFVTXNRMEZCUXp0UlFVTjZSQ3hEUVVGRExFTkJRVU03U1VGRFNpeERRVUZETzBsQlJVOHNTMEZCU3l4RFFVRkRMRzlDUVVGdlFpeERRVU5vUXl4UFFVRXdReXhGUVVNeFF5eFBRVUZQTzFGQlJWQXNkME5CUVhkRE8xRkJRM2hETEUxQlFVMHNVMEZCVXl4SFFVRkhMRVZCUVdsQ0xFTkJRVU03VVVGRGNFTXNVMEZCVXl4RFFVRkRMRlZCUVZVc1IwRkJSeXhQUVVGUExFTkJRVU03VVVGREwwSXNVMEZCVXl4RFFVRkRMRlZCUVZVc1IwRkJSeXhOUVVGTkxFTkJRVU1zVDBGQlR5eERRVUZETEZOQlFWTXNRMEZCUXl4RFFVRkRPMUZCUTJwRUxGTkJRVk1zUTBGQlF5eFpRVUZaTEVkQlFVY3NUMEZCVHl4RFFVRkRMRVZCUVVVc1EwRkJRenRSUVVOd1F5eE5RVUZOTEZkQlFWY3NSMEZCUnl4SlFVRkpMRWxCUVVrc1EwRkJReXhQUVVGUExFTkJRVU1zVTBGQlV5eERRVUZETEVOQlFVTTdVVUZEYUVRc1UwRkJVeXhEUVVGRExGVkJRVlVzUjBGQlJ5eFhRVUZYTEVOQlFVTXNWMEZCVnl4RlFVRkZMRU5CUVVNN1VVRkZha1FzWjBKQlFXZENPMUZCUTJoQ0xFMUJRVTBzUjBGQlJ5eEhRVUZITEVsQlFVa3NSMEZCUnl4RFFVRkRMRTlCUVU4c1EwRkJReXhIUVVGSExFTkJRVU1zUTBGQlF6dFJRVU5xUXl4VFFVRlRMRU5CUVVNc1VVRkJVU3hIUVVGSExFZEJRVWNzUTBGQlF5eFJRVUZSTEVOQlFVTTdVVUZEYkVNc1RVRkJUU3hWUVVGVkxFZEJRVWNzVDBGQlR5eERRVUZETEVkQlFVY3NRMEZCUXl4UFFVRlBMRU5CUVVNc1UwRkJVeXhEUVVGRExGRkJRVkVzUlVGQlJUdFpRVU42UkN4blFrRkJaMEk3VTBGRGFrSXNRMEZCUXl4RFFVRkRPMUZCUTBnc1ZVRkJWU3hEUVVGRExFbEJRVWtzUTBGQlF5eEpRVUZKTEVOQlFVTXNjVUpCUVhGQ0xFTkJRVU1zVTBGQlV5eEZRVUZGTEVsQlFVa3NRMEZCUXl4WlFVRlpMRU5CUVVNc1EwRkJReXhEUVVGRE8wbEJRelZGTEVOQlFVTTdRMEZEUmlKOSIsImltcG9ydCB7IGluY3JlbWVudGVkRXZlbnRPcmRpbmFsIH0gZnJvbSBcIi4uL2xpYi9leHRlbnNpb24tc2Vzc2lvbi1ldmVudC1vcmRpbmFsXCI7XG5pbXBvcnQgeyBleHRlbnNpb25TZXNzaW9uVXVpZCB9IGZyb20gXCIuLi9saWIvZXh0ZW5zaW9uLXNlc3Npb24tdXVpZFwiO1xuaW1wb3J0IHsgSHR0cFBvc3RQYXJzZXIgfSBmcm9tIFwiLi4vbGliL2h0dHAtcG9zdC1wYXJzZXJcIjtcbmltcG9ydCB7IFBlbmRpbmdSZXF1ZXN0IH0gZnJvbSBcIi4uL2xpYi9wZW5kaW5nLXJlcXVlc3RcIjtcbmltcG9ydCB7IFBlbmRpbmdSZXNwb25zZSB9IGZyb20gXCIuLi9saWIvcGVuZGluZy1yZXNwb25zZVwiO1xuaW1wb3J0IHsgYm9vbFRvSW50LCBlc2NhcGVTdHJpbmcsIGVzY2FwZVVybCB9IGZyb20gXCIuLi9saWIvc3RyaW5nLXV0aWxzXCI7XG4vKipcbiAqIE5vdGU6IERpZmZlcmVudCBwYXJ0cyBvZiB0aGUgZGVzaXJlZCBpbmZvcm1hdGlvbiBhcnJpdmVzIGluIGRpZmZlcmVudCBldmVudHMgYXMgcGVyIGJlbG93OlxuICogcmVxdWVzdCA9IGhlYWRlcnMgaW4gb25CZWZvcmVTZW5kSGVhZGVycyArIGJvZHkgaW4gb25CZWZvcmVSZXF1ZXN0XG4gKiByZXNwb25zZSA9IGhlYWRlcnMgaW4gb25Db21wbGV0ZWQgKyBib2R5IHZpYSBhIG9uQmVmb3JlUmVxdWVzdCBmaWx0ZXJcbiAqIHJlZGlyZWN0ID0gb3JpZ2luYWwgcmVxdWVzdCBoZWFkZXJzK2JvZHksIGZvbGxvd2VkIGJ5IGEgb25CZWZvcmVSZWRpcmVjdCBhbmQgdGhlbiBhIG5ldyBzZXQgb2YgcmVxdWVzdCBoZWFkZXJzK2JvZHkgYW5kIHJlc3BvbnNlIGhlYWRlcnMrYm9keVxuICogRG9jczogaHR0cHM6Ly9kZXZlbG9wZXIubW96aWxsYS5vcmcvZW4tVVMvZG9jcy9Vc2VyOndiYW1iZXJnL3dlYlJlcXVlc3QuUmVxdWVzdERldGFpbHNcbiAqL1xuY29uc3QgYWxsVHlwZXMgPSBbXG4gICAgXCJiZWFjb25cIixcbiAgICBcImNzcF9yZXBvcnRcIixcbiAgICBcImZvbnRcIixcbiAgICBcImltYWdlXCIsXG4gICAgXCJpbWFnZXNldFwiLFxuICAgIFwibWFpbl9mcmFtZVwiLFxuICAgIFwibWVkaWFcIixcbiAgICBcIm9iamVjdFwiLFxuICAgIFwib2JqZWN0X3N1YnJlcXVlc3RcIixcbiAgICBcInBpbmdcIixcbiAgICBcInNjcmlwdFwiLFxuICAgIFwic3BlY3VsYXRpdmVcIixcbiAgICBcInN0eWxlc2hlZXRcIixcbiAgICBcInN1Yl9mcmFtZVwiLFxuICAgIFwid2ViX21hbmlmZXN0XCIsXG4gICAgXCJ3ZWJzb2NrZXRcIixcbiAgICBcInhtbF9kdGRcIixcbiAgICBcInhtbGh0dHByZXF1ZXN0XCIsXG4gICAgXCJ4c2x0XCIsXG4gICAgXCJvdGhlclwiLFxuXTtcbmV4cG9ydCB7IGFsbFR5cGVzIH07XG5leHBvcnQgY2xhc3MgSHR0cEluc3RydW1lbnQge1xuICAgIGRhdGFSZWNlaXZlcjtcbiAgICBwZW5kaW5nUmVxdWVzdHMgPSB7fTtcbiAgICBwZW5kaW5nUmVzcG9uc2VzID0ge307XG4gICAgb25CZWZvcmVSZXF1ZXN0TGlzdGVuZXI7XG4gICAgb25CZWZvcmVTZW5kSGVhZGVyc0xpc3RlbmVyO1xuICAgIG9uQmVmb3JlUmVkaXJlY3RMaXN0ZW5lcjtcbiAgICBvbkNvbXBsZXRlZExpc3RlbmVyO1xuICAgIGNvbnN0cnVjdG9yKGRhdGFSZWNlaXZlcikge1xuICAgICAgICB0aGlzLmRhdGFSZWNlaXZlciA9IGRhdGFSZWNlaXZlcjtcbiAgICB9XG4gICAgcnVuKGNyYXdsSUQsIHNhdmVDb250ZW50T3B0aW9uKSB7XG4gICAgICAgIGNvbnN0IGZpbHRlciA9IHsgdXJsczogW1wiPGFsbF91cmxzPlwiXSwgdHlwZXM6IGFsbFR5cGVzIH07XG4gICAgICAgIGNvbnN0IHJlcXVlc3RTdGVtc0Zyb21FeHRlbnNpb24gPSAoZGV0YWlscykgPT4ge1xuICAgICAgICAgICAgcmV0dXJuIChkZXRhaWxzLm9yaWdpblVybCAmJiBkZXRhaWxzLm9yaWdpblVybC5pbmRleE9mKFwibW96LWV4dGVuc2lvbjovL1wiKSA+IC0xKTtcbiAgICAgICAgfTtcbiAgICAgICAgLypcbiAgICAgICAgICogQXR0YWNoIGhhbmRsZXJzIHRvIGV2ZW50IGxpc3RlbmVyc1xuICAgICAgICAgKi9cbiAgICAgICAgdGhpcy5vbkJlZm9yZVJlcXVlc3RMaXN0ZW5lciA9IChkZXRhaWxzKSA9PiB7XG4gICAgICAgICAgICBjb25zdCBibG9ja2luZ1Jlc3BvbnNlVGhhdERvZXNOb3RoaW5nID0ge307XG4gICAgICAgICAgICAvLyBJZ25vcmUgcmVxdWVzdHMgbWFkZSBieSBleHRlbnNpb25zXG4gICAgICAgICAgICBpZiAocmVxdWVzdFN0ZW1zRnJvbUV4dGVuc2lvbihkZXRhaWxzKSkge1xuICAgICAgICAgICAgICAgIHJldHVybiBibG9ja2luZ1Jlc3BvbnNlVGhhdERvZXNOb3RoaW5nO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgY29uc3QgcGVuZGluZ1JlcXVlc3QgPSB0aGlzLmdldFBlbmRpbmdSZXF1ZXN0KGRldGFpbHMucmVxdWVzdElkKTtcbiAgICAgICAgICAgIHBlbmRpbmdSZXF1ZXN0LnJlc29sdmVPbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHMoZGV0YWlscyk7XG4gICAgICAgICAgICBjb25zdCBwZW5kaW5nUmVzcG9uc2UgPSB0aGlzLmdldFBlbmRpbmdSZXNwb25zZShkZXRhaWxzLnJlcXVlc3RJZCk7XG4gICAgICAgICAgICBwZW5kaW5nUmVzcG9uc2UucmVzb2x2ZU9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscyhkZXRhaWxzKTtcbiAgICAgICAgICAgIGlmICh0aGlzLnNob3VsZFNhdmVDb250ZW50KHNhdmVDb250ZW50T3B0aW9uLCBkZXRhaWxzLnR5cGUpKSB7XG4gICAgICAgICAgICAgICAgcGVuZGluZ1Jlc3BvbnNlLmFkZFJlc3BvbnNlUmVzcG9uc2VCb2R5TGlzdGVuZXIoZGV0YWlscyk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICByZXR1cm4gYmxvY2tpbmdSZXNwb25zZVRoYXREb2VzTm90aGluZztcbiAgICAgICAgfTtcbiAgICAgICAgYnJvd3Nlci53ZWJSZXF1ZXN0Lm9uQmVmb3JlUmVxdWVzdC5hZGRMaXN0ZW5lcih0aGlzLm9uQmVmb3JlUmVxdWVzdExpc3RlbmVyLCBmaWx0ZXIsIHRoaXMuaXNDb250ZW50U2F2aW5nRW5hYmxlZChzYXZlQ29udGVudE9wdGlvbilcbiAgICAgICAgICAgID8gW1wicmVxdWVzdEJvZHlcIiwgXCJibG9ja2luZ1wiXVxuICAgICAgICAgICAgOiBbXCJyZXF1ZXN0Qm9keVwiXSk7XG4gICAgICAgIHRoaXMub25CZWZvcmVTZW5kSGVhZGVyc0xpc3RlbmVyID0gKGRldGFpbHMpID0+IHtcbiAgICAgICAgICAgIC8vIElnbm9yZSByZXF1ZXN0cyBtYWRlIGJ5IGV4dGVuc2lvbnNcbiAgICAgICAgICAgIGlmIChyZXF1ZXN0U3RlbXNGcm9tRXh0ZW5zaW9uKGRldGFpbHMpKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgY29uc3QgcGVuZGluZ1JlcXVlc3QgPSB0aGlzLmdldFBlbmRpbmdSZXF1ZXN0KGRldGFpbHMucmVxdWVzdElkKTtcbiAgICAgICAgICAgIHBlbmRpbmdSZXF1ZXN0LnJlc29sdmVPbkJlZm9yZVNlbmRIZWFkZXJzRXZlbnREZXRhaWxzKGRldGFpbHMpO1xuICAgICAgICAgICAgdGhpcy5vbkJlZm9yZVNlbmRIZWFkZXJzSGFuZGxlcihkZXRhaWxzLCBjcmF3bElELCBpbmNyZW1lbnRlZEV2ZW50T3JkaW5hbCgpKTtcbiAgICAgICAgfTtcbiAgICAgICAgYnJvd3Nlci53ZWJSZXF1ZXN0Lm9uQmVmb3JlU2VuZEhlYWRlcnMuYWRkTGlzdGVuZXIodGhpcy5vbkJlZm9yZVNlbmRIZWFkZXJzTGlzdGVuZXIsIGZpbHRlciwgW1wicmVxdWVzdEhlYWRlcnNcIl0pO1xuICAgICAgICB0aGlzLm9uQmVmb3JlUmVkaXJlY3RMaXN0ZW5lciA9IChkZXRhaWxzKSA9PiB7XG4gICAgICAgICAgICAvLyBJZ25vcmUgcmVxdWVzdHMgbWFkZSBieSBleHRlbnNpb25zXG4gICAgICAgICAgICBpZiAocmVxdWVzdFN0ZW1zRnJvbUV4dGVuc2lvbihkZXRhaWxzKSkge1xuICAgICAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIHRoaXMub25CZWZvcmVSZWRpcmVjdEhhbmRsZXIoZGV0YWlscywgY3Jhd2xJRCwgaW5jcmVtZW50ZWRFdmVudE9yZGluYWwoKSk7XG4gICAgICAgIH07XG4gICAgICAgIGJyb3dzZXIud2ViUmVxdWVzdC5vbkJlZm9yZVJlZGlyZWN0LmFkZExpc3RlbmVyKHRoaXMub25CZWZvcmVSZWRpcmVjdExpc3RlbmVyLCBmaWx0ZXIsIFtcInJlc3BvbnNlSGVhZGVyc1wiXSk7XG4gICAgICAgIHRoaXMub25Db21wbGV0ZWRMaXN0ZW5lciA9IChkZXRhaWxzKSA9PiB7XG4gICAgICAgICAgICAvLyBJZ25vcmUgcmVxdWVzdHMgbWFkZSBieSBleHRlbnNpb25zXG4gICAgICAgICAgICBpZiAocmVxdWVzdFN0ZW1zRnJvbUV4dGVuc2lvbihkZXRhaWxzKSkge1xuICAgICAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGNvbnN0IHBlbmRpbmdSZXNwb25zZSA9IHRoaXMuZ2V0UGVuZGluZ1Jlc3BvbnNlKGRldGFpbHMucmVxdWVzdElkKTtcbiAgICAgICAgICAgIHBlbmRpbmdSZXNwb25zZS5yZXNvbHZlT25Db21wbGV0ZWRFdmVudERldGFpbHMoZGV0YWlscyk7XG4gICAgICAgICAgICB0aGlzLm9uQ29tcGxldGVkSGFuZGxlcihkZXRhaWxzLCBjcmF3bElELCBpbmNyZW1lbnRlZEV2ZW50T3JkaW5hbCgpLCBzYXZlQ29udGVudE9wdGlvbik7XG4gICAgICAgIH07XG4gICAgICAgIGJyb3dzZXIud2ViUmVxdWVzdC5vbkNvbXBsZXRlZC5hZGRMaXN0ZW5lcih0aGlzLm9uQ29tcGxldGVkTGlzdGVuZXIsIGZpbHRlciwgW1wicmVzcG9uc2VIZWFkZXJzXCJdKTtcbiAgICB9XG4gICAgY2xlYW51cCgpIHtcbiAgICAgICAgaWYgKHRoaXMub25CZWZvcmVSZXF1ZXN0TGlzdGVuZXIpIHtcbiAgICAgICAgICAgIGJyb3dzZXIud2ViUmVxdWVzdC5vbkJlZm9yZVJlcXVlc3QucmVtb3ZlTGlzdGVuZXIodGhpcy5vbkJlZm9yZVJlcXVlc3RMaXN0ZW5lcik7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKHRoaXMub25CZWZvcmVTZW5kSGVhZGVyc0xpc3RlbmVyKSB7XG4gICAgICAgICAgICBicm93c2VyLndlYlJlcXVlc3Qub25CZWZvcmVTZW5kSGVhZGVycy5yZW1vdmVMaXN0ZW5lcih0aGlzLm9uQmVmb3JlU2VuZEhlYWRlcnNMaXN0ZW5lcik7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKHRoaXMub25CZWZvcmVSZWRpcmVjdExpc3RlbmVyKSB7XG4gICAgICAgICAgICBicm93c2VyLndlYlJlcXVlc3Qub25CZWZvcmVSZWRpcmVjdC5yZW1vdmVMaXN0ZW5lcih0aGlzLm9uQmVmb3JlUmVkaXJlY3RMaXN0ZW5lcik7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKHRoaXMub25Db21wbGV0ZWRMaXN0ZW5lcikge1xuICAgICAgICAgICAgYnJvd3Nlci53ZWJSZXF1ZXN0Lm9uQ29tcGxldGVkLnJlbW92ZUxpc3RlbmVyKHRoaXMub25Db21wbGV0ZWRMaXN0ZW5lcik7XG4gICAgICAgIH1cbiAgICB9XG4gICAgaXNDb250ZW50U2F2aW5nRW5hYmxlZChzYXZlQ29udGVudE9wdGlvbikge1xuICAgICAgICBpZiAoc2F2ZUNvbnRlbnRPcHRpb24gPT09IHRydWUpIHtcbiAgICAgICAgICAgIHJldHVybiB0cnVlO1xuICAgICAgICB9XG4gICAgICAgIGlmIChzYXZlQ29udGVudE9wdGlvbiA9PT0gZmFsc2UpIHtcbiAgICAgICAgICAgIHJldHVybiBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gdGhpcy5zYXZlQ29udGVudFJlc291cmNlVHlwZXMoc2F2ZUNvbnRlbnRPcHRpb24pLmxlbmd0aCA+IDA7XG4gICAgfVxuICAgIHNhdmVDb250ZW50UmVzb3VyY2VUeXBlcyhzYXZlQ29udGVudE9wdGlvbikge1xuICAgICAgICByZXR1cm4gc2F2ZUNvbnRlbnRPcHRpb24uc3BsaXQoXCIsXCIpO1xuICAgIH1cbiAgICAvKipcbiAgICAgKiBXZSByZWx5IG9uIHRoZSByZXNvdXJjZSB0eXBlIHRvIGZpbHRlciByZXNwb25zZXNcbiAgICAgKiBTZWU6IGh0dHBzOi8vZGV2ZWxvcGVyLm1vemlsbGEub3JnL2VuLVVTL2RvY3MvTW96aWxsYS9BZGQtb25zL1dlYkV4dGVuc2lvbnMvQVBJL3dlYlJlcXVlc3QvUmVzb3VyY2VUeXBlXG4gICAgICpcbiAgICAgKiBAcGFyYW0gc2F2ZUNvbnRlbnRPcHRpb25cbiAgICAgKiBAcGFyYW0gcmVzb3VyY2VUeXBlXG4gICAgICovXG4gICAgc2hvdWxkU2F2ZUNvbnRlbnQoc2F2ZUNvbnRlbnRPcHRpb24sIHJlc291cmNlVHlwZSkge1xuICAgICAgICBpZiAoc2F2ZUNvbnRlbnRPcHRpb24gPT09IHRydWUpIHtcbiAgICAgICAgICAgIHJldHVybiB0cnVlO1xuICAgICAgICB9XG4gICAgICAgIGlmIChzYXZlQ29udGVudE9wdGlvbiA9PT0gZmFsc2UpIHtcbiAgICAgICAgICAgIHJldHVybiBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gdGhpcy5zYXZlQ29udGVudFJlc291cmNlVHlwZXMoc2F2ZUNvbnRlbnRPcHRpb24pLmluY2x1ZGVzKHJlc291cmNlVHlwZSk7XG4gICAgfVxuICAgIGdldFBlbmRpbmdSZXF1ZXN0KHJlcXVlc3RJZCkge1xuICAgICAgICBpZiAoIXRoaXMucGVuZGluZ1JlcXVlc3RzW3JlcXVlc3RJZF0pIHtcbiAgICAgICAgICAgIHRoaXMucGVuZGluZ1JlcXVlc3RzW3JlcXVlc3RJZF0gPSBuZXcgUGVuZGluZ1JlcXVlc3QoKTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gdGhpcy5wZW5kaW5nUmVxdWVzdHNbcmVxdWVzdElkXTtcbiAgICB9XG4gICAgZ2V0UGVuZGluZ1Jlc3BvbnNlKHJlcXVlc3RJZCkge1xuICAgICAgICBpZiAoIXRoaXMucGVuZGluZ1Jlc3BvbnNlc1tyZXF1ZXN0SWRdKSB7XG4gICAgICAgICAgICB0aGlzLnBlbmRpbmdSZXNwb25zZXNbcmVxdWVzdElkXSA9IG5ldyBQZW5kaW5nUmVzcG9uc2UoKTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gdGhpcy5wZW5kaW5nUmVzcG9uc2VzW3JlcXVlc3RJZF07XG4gICAgfVxuICAgIC8qXG4gICAgICogSFRUUCBSZXF1ZXN0IEhhbmRsZXIgYW5kIEhlbHBlciBGdW5jdGlvbnNcbiAgICAgKi9cbiAgICBhc3luYyBvbkJlZm9yZVNlbmRIZWFkZXJzSGFuZGxlcihkZXRhaWxzLCBjcmF3bElELCBldmVudE9yZGluYWwpIHtcbiAgICAgICAgY29uc3QgdGFiID0gZGV0YWlscy50YWJJZCA+IC0xXG4gICAgICAgICAgICA/IGF3YWl0IGJyb3dzZXIudGFicy5nZXQoZGV0YWlscy50YWJJZClcbiAgICAgICAgICAgIDogeyB3aW5kb3dJZDogdW5kZWZpbmVkLCBpbmNvZ25pdG86IHVuZGVmaW5lZCwgdXJsOiB1bmRlZmluZWQgfTtcbiAgICAgICAgY29uc3QgdXBkYXRlID0ge307XG4gICAgICAgIHVwZGF0ZS5pbmNvZ25pdG8gPSBib29sVG9JbnQodGFiLmluY29nbml0byk7XG4gICAgICAgIHVwZGF0ZS5icm93c2VyX2lkID0gY3Jhd2xJRDtcbiAgICAgICAgdXBkYXRlLmV4dGVuc2lvbl9zZXNzaW9uX3V1aWQgPSBleHRlbnNpb25TZXNzaW9uVXVpZDtcbiAgICAgICAgdXBkYXRlLmV2ZW50X29yZGluYWwgPSBldmVudE9yZGluYWw7XG4gICAgICAgIHVwZGF0ZS53aW5kb3dfaWQgPSB0YWIud2luZG93SWQ7XG4gICAgICAgIHVwZGF0ZS50YWJfaWQgPSBkZXRhaWxzLnRhYklkO1xuICAgICAgICB1cGRhdGUuZnJhbWVfaWQgPSBkZXRhaWxzLmZyYW1lSWQ7XG4gICAgICAgIC8vIHJlcXVlc3RJZCBpcyBhIHVuaXF1ZSBpZGVudGlmaWVyIHRoYXQgY2FuIGJlIHVzZWQgdG8gbGluayByZXF1ZXN0cyBhbmQgcmVzcG9uc2VzXG4gICAgICAgIHVwZGF0ZS5yZXF1ZXN0X2lkID0gTnVtYmVyKGRldGFpbHMucmVxdWVzdElkKTtcbiAgICAgICAgY29uc3QgdXJsID0gZGV0YWlscy51cmw7XG4gICAgICAgIHVwZGF0ZS51cmwgPSBlc2NhcGVVcmwodXJsKTtcbiAgICAgICAgY29uc3QgcmVxdWVzdE1ldGhvZCA9IGRldGFpbHMubWV0aG9kO1xuICAgICAgICB1cGRhdGUubWV0aG9kID0gZXNjYXBlU3RyaW5nKHJlcXVlc3RNZXRob2QpO1xuICAgICAgICBjb25zdCBjdXJyZW50X3RpbWUgPSBuZXcgRGF0ZShkZXRhaWxzLnRpbWVTdGFtcCk7XG4gICAgICAgIHVwZGF0ZS50aW1lX3N0YW1wID0gY3VycmVudF90aW1lLnRvSVNPU3RyaW5nKCk7XG4gICAgICAgIGxldCBlbmNvZGluZ1R5cGUgPSBcIlwiO1xuICAgICAgICBsZXQgcmVmZXJyZXIgPSBcIlwiO1xuICAgICAgICBjb25zdCBoZWFkZXJzID0gW107XG4gICAgICAgIGxldCBpc09jc3AgPSBmYWxzZTtcbiAgICAgICAgaWYgKGRldGFpbHMucmVxdWVzdEhlYWRlcnMpIHtcbiAgICAgICAgICAgIGRldGFpbHMucmVxdWVzdEhlYWRlcnMubWFwKChyZXF1ZXN0SGVhZGVyKSA9PiB7XG4gICAgICAgICAgICAgICAgY29uc3QgeyBuYW1lLCB2YWx1ZSB9ID0gcmVxdWVzdEhlYWRlcjtcbiAgICAgICAgICAgICAgICBjb25zdCBoZWFkZXJfcGFpciA9IFtdO1xuICAgICAgICAgICAgICAgIGhlYWRlcl9wYWlyLnB1c2goZXNjYXBlU3RyaW5nKG5hbWUpKTtcbiAgICAgICAgICAgICAgICBoZWFkZXJfcGFpci5wdXNoKGVzY2FwZVN0cmluZyh2YWx1ZSkpO1xuICAgICAgICAgICAgICAgIGhlYWRlcnMucHVzaChoZWFkZXJfcGFpcik7XG4gICAgICAgICAgICAgICAgaWYgKG5hbWUgPT09IFwiQ29udGVudC1UeXBlXCIpIHtcbiAgICAgICAgICAgICAgICAgICAgZW5jb2RpbmdUeXBlID0gdmFsdWU7XG4gICAgICAgICAgICAgICAgICAgIGlmIChlbmNvZGluZ1R5cGUuaW5kZXhPZihcImFwcGxpY2F0aW9uL29jc3AtcmVxdWVzdFwiKSAhPT0gLTEpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGlzT2NzcCA9IHRydWU7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgaWYgKG5hbWUgPT09IFwiUmVmZXJlclwiKSB7XG4gICAgICAgICAgICAgICAgICAgIHJlZmVycmVyID0gdmFsdWU7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgICAgdXBkYXRlLnJlZmVycmVyID0gZXNjYXBlU3RyaW5nKHJlZmVycmVyKTtcbiAgICAgICAgaWYgKHJlcXVlc3RNZXRob2QgPT09IFwiUE9TVFwiICYmICFpc09jc3AgLyogZG9uJ3QgcHJvY2VzcyBPQ1NQIHJlcXVlc3RzICovKSB7XG4gICAgICAgICAgICBjb25zdCBwZW5kaW5nUmVxdWVzdCA9IHRoaXMuZ2V0UGVuZGluZ1JlcXVlc3QoZGV0YWlscy5yZXF1ZXN0SWQpO1xuICAgICAgICAgICAgY29uc3QgcmVzb2x2ZWQgPSBhd2FpdCBwZW5kaW5nUmVxdWVzdC5yZXNvbHZlZFdpdGhpblRpbWVvdXQoMTAwMCk7XG4gICAgICAgICAgICBpZiAoIXJlc29sdmVkKSB7XG4gICAgICAgICAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIubG9nRXJyb3IoXCJQZW5kaW5nIHJlcXVlc3QgdGltZWQgb3V0IHdhaXRpbmcgZm9yIGRhdGEgZnJvbSBib3RoIG9uQmVmb3JlUmVxdWVzdCBhbmQgb25CZWZvcmVTZW5kSGVhZGVycyBldmVudHNcIik7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBlbHNlIHtcbiAgICAgICAgICAgICAgICBjb25zdCBvbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHMgPSBhd2FpdCBwZW5kaW5nUmVxdWVzdC5vbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHM7XG4gICAgICAgICAgICAgICAgY29uc3QgcmVxdWVzdEJvZHkgPSBvbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHMucmVxdWVzdEJvZHk7XG4gICAgICAgICAgICAgICAgaWYgKHJlcXVlc3RCb2R5KSB7XG4gICAgICAgICAgICAgICAgICAgIGNvbnN0IHBvc3RQYXJzZXIgPSBuZXcgSHR0cFBvc3RQYXJzZXIob25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzLCB0aGlzLmRhdGFSZWNlaXZlcik7XG4gICAgICAgICAgICAgICAgICAgIGNvbnN0IHBvc3RPYmogPSBwb3N0UGFyc2VyLnBhcnNlUG9zdFJlcXVlc3QoKTtcbiAgICAgICAgICAgICAgICAgICAgLy8gQWRkIChQT1NUKSByZXF1ZXN0IGhlYWRlcnMgZnJvbSB1cGxvYWQgc3RyZWFtXG4gICAgICAgICAgICAgICAgICAgIGlmIChcInBvc3RfaGVhZGVyc1wiIGluIHBvc3RPYmopIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIC8vIE9ubHkgc3RvcmUgUE9TVCBoZWFkZXJzIHRoYXQgd2Uga25vdyBhbmQgbmVlZC4gV2UgbWF5IG1pc2ludGVycHJldCBQT1NUIGRhdGEgYXMgaGVhZGVyc1xuICAgICAgICAgICAgICAgICAgICAgICAgLy8gYXMgZGV0ZWN0aW9uIGlzIGJhc2VkIG9uIFwia2V5OnZhbHVlXCIgZm9ybWF0IChub24taGVhZGVyIFBPU1QgZGF0YSBjYW4gYmUgaW4gdGhpcyBmb3JtYXQgYXMgd2VsbClcbiAgICAgICAgICAgICAgICAgICAgICAgIGNvbnN0IGNvbnRlbnRIZWFkZXJzID0gW1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIFwiQ29udGVudC1UeXBlXCIsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgXCJDb250ZW50LURpc3Bvc2l0aW9uXCIsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgXCJDb250ZW50LUxlbmd0aFwiLFxuICAgICAgICAgICAgICAgICAgICAgICAgXTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGZvciAoY29uc3QgbmFtZSBpbiBwb3N0T2JqLnBvc3RfaGVhZGVycykge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmIChjb250ZW50SGVhZGVycy5pbmNsdWRlcyhuYW1lKSkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBjb25zdCBoZWFkZXJfcGFpciA9IFtdO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBoZWFkZXJfcGFpci5wdXNoKGVzY2FwZVN0cmluZyhuYW1lKSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGhlYWRlcl9wYWlyLnB1c2goZXNjYXBlU3RyaW5nKHBvc3RPYmoucG9zdF9oZWFkZXJzW25hbWVdKSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGhlYWRlcnMucHVzaChoZWFkZXJfcGFpcik7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgIC8vIHdlIHN0b3JlIFBPU1QgYm9keSBpbiBKU09OIGZvcm1hdCwgZXhjZXB0IHdoZW4gaXQncyBhIHN0cmluZyB3aXRob3V0IGEgKGtleS12YWx1ZSkgc3RydWN0dXJlXG4gICAgICAgICAgICAgICAgICAgIGlmIChcInBvc3RfYm9keVwiIGluIHBvc3RPYmopIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHVwZGF0ZS5wb3N0X2JvZHkgPSBwb3N0T2JqLnBvc3RfYm9keTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICBpZiAoXCJwb3N0X2JvZHlfcmF3XCIgaW4gcG9zdE9iaikge1xuICAgICAgICAgICAgICAgICAgICAgICAgdXBkYXRlLnBvc3RfYm9keV9yYXcgPSBwb3N0T2JqLnBvc3RfYm9keV9yYXc7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgdXBkYXRlLmhlYWRlcnMgPSBKU09OLnN0cmluZ2lmeShoZWFkZXJzKTtcbiAgICAgICAgLy8gQ2hlY2sgaWYgeGhyXG4gICAgICAgIGNvbnN0IGlzWEhSID0gZGV0YWlscy50eXBlID09PSBcInhtbGh0dHByZXF1ZXN0XCI7XG4gICAgICAgIHVwZGF0ZS5pc19YSFIgPSBib29sVG9JbnQoaXNYSFIpO1xuICAgICAgICAvLyBHcmFiIHRoZSB0cmlnZ2VyaW5nIGFuZCBsb2FkaW5nIFByaW5jaXBhbHNcbiAgICAgICAgbGV0IHRyaWdnZXJpbmdPcmlnaW47XG4gICAgICAgIGxldCBsb2FkaW5nT3JpZ2luO1xuICAgICAgICBpZiAoZGV0YWlscy5vcmlnaW5VcmwpIHtcbiAgICAgICAgICAgIGNvbnN0IHBhcnNlZE9yaWdpblVybCA9IG5ldyBVUkwoZGV0YWlscy5vcmlnaW5VcmwpO1xuICAgICAgICAgICAgdHJpZ2dlcmluZ09yaWdpbiA9IHBhcnNlZE9yaWdpblVybC5vcmlnaW47XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGRldGFpbHMuZG9jdW1lbnRVcmwpIHtcbiAgICAgICAgICAgIGNvbnN0IHBhcnNlZERvY3VtZW50VXJsID0gbmV3IFVSTChkZXRhaWxzLmRvY3VtZW50VXJsKTtcbiAgICAgICAgICAgIGxvYWRpbmdPcmlnaW4gPSBwYXJzZWREb2N1bWVudFVybC5vcmlnaW47XG4gICAgICAgIH1cbiAgICAgICAgdXBkYXRlLnRyaWdnZXJpbmdfb3JpZ2luID0gZXNjYXBlU3RyaW5nKHRyaWdnZXJpbmdPcmlnaW4pO1xuICAgICAgICB1cGRhdGUubG9hZGluZ19vcmlnaW4gPSBlc2NhcGVTdHJpbmcobG9hZGluZ09yaWdpbik7XG4gICAgICAgIC8vIGxvYWRpbmdEb2N1bWVudCdzIGhyZWZcbiAgICAgICAgLy8gVGhlIGxvYWRpbmdEb2N1bWVudCBpcyB0aGUgZG9jdW1lbnQgdGhlIGVsZW1lbnQgcmVzaWRlcywgcmVnYXJkbGVzcyBvZlxuICAgICAgICAvLyBob3cgdGhlIGxvYWQgd2FzIHRyaWdnZXJlZC5cbiAgICAgICAgY29uc3QgbG9hZGluZ0hyZWYgPSBkZXRhaWxzLmRvY3VtZW50VXJsO1xuICAgICAgICB1cGRhdGUubG9hZGluZ19ocmVmID0gZXNjYXBlU3RyaW5nKGxvYWRpbmdIcmVmKTtcbiAgICAgICAgLy8gcmVzb3VyY2VUeXBlIG9mIHRoZSByZXF1ZXN0aW5nIG5vZGUuIFRoaXMgaXMgc2V0IGJ5IHRoZSB0eXBlIG9mXG4gICAgICAgIC8vIG5vZGUgbWFraW5nIHRoZSByZXF1ZXN0IChpLmUuIGFuIDxpbWcgc3JjPS4uLj4gbm9kZSB3aWxsIHNldCB0byB0eXBlIFwiaW1hZ2VcIikuXG4gICAgICAgIC8vIERvY3VtZW50YXRpb246XG4gICAgICAgIC8vIGh0dHBzOi8vZGV2ZWxvcGVyLm1vemlsbGEub3JnL2VuLVVTL2RvY3MvTW96aWxsYS9BZGQtb25zL1dlYkV4dGVuc2lvbnMvQVBJL3dlYlJlcXVlc3QvUmVzb3VyY2VUeXBlXG4gICAgICAgIHVwZGF0ZS5yZXNvdXJjZV90eXBlID0gZGV0YWlscy50eXBlO1xuICAgICAgICAvKlxuICAgICAgICAvLyBUT0RPOiBSZWZhY3RvciB0byBjb3JyZXNwb25kaW5nIHdlYmV4dCBsb2dpYyBvciBkaXNjYXJkXG4gICAgICAgIGNvbnN0IFRoaXJkUGFydHlVdGlsID0gQ2NbXCJAbW96aWxsYS5vcmcvdGhpcmRwYXJ0eXV0aWw7MVwiXS5nZXRTZXJ2aWNlKFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIENpLm1veklUaGlyZFBhcnR5VXRpbCk7XG4gICAgICAgIC8vIERvIHRoaXJkLXBhcnR5IGNoZWNrc1xuICAgICAgICAvLyBUaGVzZSBzcGVjaWZpYyBjaGVja3MgYXJlIGRvbmUgYmVjYXVzZSBpdCdzIHdoYXQncyB1c2VkIGluIFRyYWNraW5nIFByb3RlY3Rpb25cbiAgICAgICAgLy8gU2VlOiBodHRwOi8vc2VhcmNoZm94Lm9yZy9tb3ppbGxhLWNlbnRyYWwvc291cmNlL25ldHdlcmsvYmFzZS9uc0NoYW5uZWxDbGFzc2lmaWVyLmNwcCMxMDdcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICBjb25zdCBpc1RoaXJkUGFydHlDaGFubmVsID0gVGhpcmRQYXJ0eVV0aWwuaXNUaGlyZFBhcnR5Q2hhbm5lbChkZXRhaWxzKTtcbiAgICAgICAgICBjb25zdCB0b3BXaW5kb3cgPSBUaGlyZFBhcnR5VXRpbC5nZXRUb3BXaW5kb3dGb3JDaGFubmVsKGRldGFpbHMpO1xuICAgICAgICAgIGNvbnN0IHRvcFVSSSA9IFRoaXJkUGFydHlVdGlsLmdldFVSSUZyb21XaW5kb3codG9wV2luZG93KTtcbiAgICAgICAgICBpZiAodG9wVVJJKSB7XG4gICAgICAgICAgICBjb25zdCB0b3BVcmwgPSB0b3BVUkkuc3BlYztcbiAgICAgICAgICAgIGNvbnN0IGNoYW5uZWxVUkkgPSBkZXRhaWxzLlVSSTtcbiAgICAgICAgICAgIGNvbnN0IGlzVGhpcmRQYXJ0eVRvVG9wV2luZG93ID0gVGhpcmRQYXJ0eVV0aWwuaXNUaGlyZFBhcnR5VVJJKFxuICAgICAgICAgICAgICBjaGFubmVsVVJJLFxuICAgICAgICAgICAgICB0b3BVUkksXG4gICAgICAgICAgICApO1xuICAgICAgICAgICAgdXBkYXRlLmlzX3RoaXJkX3BhcnR5X3RvX3RvcF93aW5kb3cgPSBpc1RoaXJkUGFydHlUb1RvcFdpbmRvdztcbiAgICAgICAgICAgIHVwZGF0ZS5pc190aGlyZF9wYXJ0eV9jaGFubmVsID0gaXNUaGlyZFBhcnR5Q2hhbm5lbDtcbiAgICAgICAgICB9XG4gICAgICAgIH0gY2F0Y2ggKGFuRXJyb3IpIHtcbiAgICAgICAgICAvLyBFeGNlcHRpb25zIGV4cGVjdGVkIGZvciBjaGFubmVscyB0cmlnZ2VyZWQgb3IgbG9hZGluZyBpbiBhXG4gICAgICAgICAgLy8gTnVsbFByaW5jaXBhbCBvciBTeXN0ZW1QcmluY2lwYWwuIFRoZXkgYXJlIGFsc28gZXhwZWN0ZWQgZm9yIGZhdmljb25cbiAgICAgICAgICAvLyBsb2Fkcywgd2hpY2ggd2UgYXR0ZW1wdCB0byBmaWx0ZXIuIERlcGVuZGluZyBvbiB0aGUgbmFtaW5nLCBzb21lIGZhdmljb25zXG4gICAgICAgICAgLy8gbWF5IGNvbnRpbnVlIHRvIGxlYWQgdG8gZXJyb3IgbG9ncy5cbiAgICAgICAgICBpZiAoXG4gICAgICAgICAgICB1cGRhdGUudHJpZ2dlcmluZ19vcmlnaW4gIT09IFwiW1N5c3RlbSBQcmluY2lwYWxdXCIgJiZcbiAgICAgICAgICAgIHVwZGF0ZS50cmlnZ2VyaW5nX29yaWdpbiAhPT0gdW5kZWZpbmVkICYmXG4gICAgICAgICAgICB1cGRhdGUubG9hZGluZ19vcmlnaW4gIT09IFwiW1N5c3RlbSBQcmluY2lwYWxdXCIgJiZcbiAgICAgICAgICAgIHVwZGF0ZS5sb2FkaW5nX29yaWdpbiAhPT0gdW5kZWZpbmVkICYmXG4gICAgICAgICAgICAhdXBkYXRlLnVybC5lbmRzV2l0aChcImljb1wiKVxuICAgICAgICAgICkge1xuICAgICAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIubG9nRXJyb3IoXG4gICAgICAgICAgICAgIFwiRXJyb3Igd2hpbGUgcmV0cmlldmluZyBhZGRpdGlvbmFsIGNoYW5uZWwgaW5mb3JtYXRpb24gZm9yIFVSTDogXCIgK1xuICAgICAgICAgICAgICBcIlxcblwiICtcbiAgICAgICAgICAgICAgdXBkYXRlLnVybCArXG4gICAgICAgICAgICAgIFwiXFxuIEVycm9yIHRleHQ6XCIgK1xuICAgICAgICAgICAgICBKU09OLnN0cmluZ2lmeShhbkVycm9yKSxcbiAgICAgICAgICAgICk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgICovXG4gICAgICAgIHVwZGF0ZS50b3BfbGV2ZWxfdXJsID0gZXNjYXBlVXJsKHRoaXMuZ2V0RG9jdW1lbnRVcmxGb3JSZXF1ZXN0KGRldGFpbHMpKTtcbiAgICAgICAgdXBkYXRlLnBhcmVudF9mcmFtZV9pZCA9IGRldGFpbHMucGFyZW50RnJhbWVJZDtcbiAgICAgICAgdXBkYXRlLmZyYW1lX2FuY2VzdG9ycyA9IGVzY2FwZVN0cmluZyhKU09OLnN0cmluZ2lmeShkZXRhaWxzLmZyYW1lQW5jZXN0b3JzKSk7XG4gICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLnNhdmVSZWNvcmQoXCJodHRwX3JlcXVlc3RzXCIsIHVwZGF0ZSk7XG4gICAgfVxuICAgIC8qKlxuICAgICAqIENvZGUgdGFrZW4gYW5kIGFkYXB0ZWQgZnJvbVxuICAgICAqIGh0dHBzOi8vZ2l0aHViLmNvbS9FRkZvcmcvcHJpdmFjeWJhZGdlci9wdWxsLzIxOTgvZmlsZXNcbiAgICAgKlxuICAgICAqIEdldHMgdGhlIFVSTCBmb3IgYSBnaXZlbiByZXF1ZXN0J3MgdG9wLWxldmVsIGRvY3VtZW50LlxuICAgICAqXG4gICAgICogVGhlIHJlcXVlc3QncyBkb2N1bWVudCBtYXkgYmUgZGlmZmVyZW50IGZyb20gdGhlIGN1cnJlbnQgdG9wLWxldmVsIGRvY3VtZW50XG4gICAgICogbG9hZGVkIGluIHRhYiBhcyByZXF1ZXN0cyBjYW4gY29tZSBvdXQgb2Ygb3JkZXI6XG4gICAgICpcbiAgICAgKiBAcGFyYW0ge1dlYlJlcXVlc3RPbkJlZm9yZVNlbmRIZWFkZXJzRXZlbnREZXRhaWxzfSBkZXRhaWxzXG4gICAgICpcbiAgICAgKiBAcmV0dXJuIHs/U3RyaW5nfSB0aGUgVVJMIGZvciB0aGUgcmVxdWVzdCdzIHRvcC1sZXZlbCBkb2N1bWVudFxuICAgICAqL1xuICAgIGdldERvY3VtZW50VXJsRm9yUmVxdWVzdChkZXRhaWxzKSB7XG4gICAgICAgIGxldCB1cmwgPSBcIlwiO1xuICAgICAgICBpZiAoZGV0YWlscy50eXBlID09PSBcIm1haW5fZnJhbWVcIikge1xuICAgICAgICAgICAgLy8gVXJsIG9mIHRoZSB0b3AtbGV2ZWwgZG9jdW1lbnQgaXRzZWxmLlxuICAgICAgICAgICAgdXJsID0gZGV0YWlscy51cmw7XG4gICAgICAgIH1cbiAgICAgICAgZWxzZSBpZiAoZGV0YWlscy5oYXNPd25Qcm9wZXJ0eShcImZyYW1lQW5jZXN0b3JzXCIpKSB7XG4gICAgICAgICAgICAvLyBJbiBjYXNlIG9mIG5lc3RlZCBmcmFtZXMsIHJldHJpZXZlIHVybCBmcm9tIHRvcC1tb3N0IGFuY2VzdG9yLlxuICAgICAgICAgICAgLy8gSWYgZnJhbWVBbmNlc3RvcnMgPT0gW10sIHJlcXVlc3QgY29tZXMgZnJvbSB0aGUgdG9wLWxldmVsLWRvY3VtZW50LlxuICAgICAgICAgICAgdXJsID0gZGV0YWlscy5mcmFtZUFuY2VzdG9ycy5sZW5ndGhcbiAgICAgICAgICAgICAgICA/IGRldGFpbHMuZnJhbWVBbmNlc3RvcnNbZGV0YWlscy5mcmFtZUFuY2VzdG9ycy5sZW5ndGggLSAxXS51cmxcbiAgICAgICAgICAgICAgICA6IGRldGFpbHMuZG9jdW1lbnRVcmw7XG4gICAgICAgIH1cbiAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAvLyB0eXBlICE9ICdtYWluX2ZyYW1lJyBhbmQgZnJhbWVBbmNlc3RvcnMgPT0gdW5kZWZpbmVkXG4gICAgICAgICAgICAvLyBGb3IgZXhhbXBsZSBzZXJ2aWNlIHdvcmtlcnM6IGh0dHBzOi8vYnVnemlsbGEubW96aWxsYS5vcmcvc2hvd19idWcuY2dpP2lkPTE0NzA1MzcjYzEzXG4gICAgICAgICAgICB1cmwgPSBkZXRhaWxzLmRvY3VtZW50VXJsO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiB1cmw7XG4gICAgfVxuICAgIGFzeW5jIG9uQmVmb3JlUmVkaXJlY3RIYW5kbGVyKGRldGFpbHMsIGNyYXdsSUQsIGV2ZW50T3JkaW5hbCkge1xuICAgICAgICAvKlxuICAgICAgICBjb25zb2xlLmxvZyhcbiAgICAgICAgICBcIm9uQmVmb3JlUmVkaXJlY3RIYW5kbGVyIChwcmV2aW91c2x5IGh0dHBSZXF1ZXN0SGFuZGxlcilcIixcbiAgICAgICAgICBkZXRhaWxzLFxuICAgICAgICAgIGNyYXdsSUQsXG4gICAgICAgICk7XG4gICAgICAgICovXG4gICAgICAgIC8vIFNhdmUgSFRUUCByZWRpcmVjdCBldmVudHNcbiAgICAgICAgLy8gRXZlbnRzIGFyZSBzYXZlZCB0byB0aGUgYGh0dHBfcmVkaXJlY3RzYCB0YWJsZVxuICAgICAgICAvKlxuICAgICAgICAvLyBUT0RPOiBSZWZhY3RvciB0byBjb3JyZXNwb25kaW5nIHdlYmV4dCBsb2dpYyBvciBkaXNjYXJkXG4gICAgICAgIC8vIEV2ZW50cyBhcmUgc2F2ZWQgdG8gdGhlIGBodHRwX3JlZGlyZWN0c2AgdGFibGUsIGFuZCBtYXAgdGhlIG9sZFxuICAgICAgICAvLyByZXF1ZXN0L3Jlc3BvbnNlIGNoYW5uZWwgaWQgdG8gdGhlIG5ldyByZXF1ZXN0L3Jlc3BvbnNlIGNoYW5uZWwgaWQuXG4gICAgICAgIC8vIEltcGxlbWVudGF0aW9uIGJhc2VkIG9uOiBodHRwczovL3N0YWNrb3ZlcmZsb3cuY29tL2EvMTEyNDA2MjdcbiAgICAgICAgY29uc3Qgb2xkTm90aWZpY2F0aW9ucyA9IGRldGFpbHMubm90aWZpY2F0aW9uQ2FsbGJhY2tzO1xuICAgICAgICBsZXQgb2xkRXZlbnRTaW5rID0gbnVsbDtcbiAgICAgICAgZGV0YWlscy5ub3RpZmljYXRpb25DYWxsYmFja3MgPSB7XG4gICAgICAgICAgUXVlcnlJbnRlcmZhY2U6IFhQQ09NVXRpbHMuZ2VuZXJhdGVRSShbXG4gICAgICAgICAgICBDaS5uc0lJbnRlcmZhY2VSZXF1ZXN0b3IsXG4gICAgICAgICAgICBDaS5uc0lDaGFubmVsRXZlbnRTaW5rLFxuICAgICAgICAgIF0pLFxuICAgIFxuICAgICAgICAgIGdldEludGVyZmFjZShpaWQpIHtcbiAgICAgICAgICAgIC8vIFdlIGFyZSBvbmx5IGludGVyZXN0ZWQgaW4gbnNJQ2hhbm5lbEV2ZW50U2luayxcbiAgICAgICAgICAgIC8vIHJldHVybiB0aGUgb2xkIGNhbGxiYWNrcyBmb3IgYW55IG90aGVyIGludGVyZmFjZSByZXF1ZXN0cy5cbiAgICAgICAgICAgIGlmIChpaWQuZXF1YWxzKENpLm5zSUNoYW5uZWxFdmVudFNpbmspKSB7XG4gICAgICAgICAgICAgIHRyeSB7XG4gICAgICAgICAgICAgICAgb2xkRXZlbnRTaW5rID0gb2xkTm90aWZpY2F0aW9ucy5RdWVyeUludGVyZmFjZShpaWQpO1xuICAgICAgICAgICAgICB9IGNhdGNoIChhbkVycm9yKSB7XG4gICAgICAgICAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIubG9nRXJyb3IoXG4gICAgICAgICAgICAgICAgICBcIkVycm9yIGR1cmluZyBjYWxsIHRvIGN1c3RvbSBub3RpZmljYXRpb25DYWxsYmFja3M6OmdldEludGVyZmFjZS5cIiArXG4gICAgICAgICAgICAgICAgICAgIEpTT04uc3RyaW5naWZ5KGFuRXJyb3IpLFxuICAgICAgICAgICAgICAgICk7XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgcmV0dXJuIHRoaXM7XG4gICAgICAgICAgICB9XG4gICAgXG4gICAgICAgICAgICBpZiAob2xkTm90aWZpY2F0aW9ucykge1xuICAgICAgICAgICAgICByZXR1cm4gb2xkTm90aWZpY2F0aW9ucy5nZXRJbnRlcmZhY2UoaWlkKTtcbiAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgIHRocm93IENyLk5TX0VSUk9SX05PX0lOVEVSRkFDRTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9LFxuICAgIFxuICAgICAgICAgIGFzeW5jT25DaGFubmVsUmVkaXJlY3Qob2xkQ2hhbm5lbCwgbmV3Q2hhbm5lbCwgZmxhZ3MsIGNhbGxiYWNrKSB7XG4gICAgXG4gICAgICAgICAgICBuZXdDaGFubmVsLlF1ZXJ5SW50ZXJmYWNlKENpLm5zSUh0dHBDaGFubmVsKTtcbiAgICBcbiAgICAgICAgICAgIGNvbnN0IGh0dHBSZWRpcmVjdDogSHR0cFJlZGlyZWN0ID0ge1xuICAgICAgICAgICAgICBicm93c2VyX2lkOiBjcmF3bElELFxuICAgICAgICAgICAgICBvbGRfcmVxdWVzdF9pZDogb2xkQ2hhbm5lbC5jaGFubmVsSWQsXG4gICAgICAgICAgICAgIG5ld19yZXF1ZXN0X2lkOiBuZXdDaGFubmVsLmNoYW5uZWxJZCxcbiAgICAgICAgICAgICAgdGltZV9zdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICAgICAgfTtcbiAgICAgICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLnNhdmVSZWNvcmQoXCJodHRwX3JlZGlyZWN0c1wiLCBodHRwUmVkaXJlY3QpO1xuICAgIFxuICAgICAgICAgICAgaWYgKG9sZEV2ZW50U2luaykge1xuICAgICAgICAgICAgICBvbGRFdmVudFNpbmsuYXN5bmNPbkNoYW5uZWxSZWRpcmVjdChcbiAgICAgICAgICAgICAgICBvbGRDaGFubmVsLFxuICAgICAgICAgICAgICAgIG5ld0NoYW5uZWwsXG4gICAgICAgICAgICAgICAgZmxhZ3MsXG4gICAgICAgICAgICAgICAgY2FsbGJhY2ssXG4gICAgICAgICAgICAgICk7XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICBjYWxsYmFjay5vblJlZGlyZWN0VmVyaWZ5Q2FsbGJhY2soQ3IuTlNfT0spO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH0sXG4gICAgICAgIH07XG4gICAgICAgICovXG4gICAgICAgIGNvbnN0IHJlc3BvbnNlU3RhdHVzID0gZGV0YWlscy5zdGF0dXNDb2RlO1xuICAgICAgICBjb25zdCByZXNwb25zZVN0YXR1c1RleHQgPSBkZXRhaWxzLnN0YXR1c0xpbmU7XG4gICAgICAgIGNvbnN0IHRhYiA9IGRldGFpbHMudGFiSWQgPiAtMVxuICAgICAgICAgICAgPyBhd2FpdCBicm93c2VyLnRhYnMuZ2V0KGRldGFpbHMudGFiSWQpXG4gICAgICAgICAgICA6IHsgd2luZG93SWQ6IHVuZGVmaW5lZCwgaW5jb2duaXRvOiB1bmRlZmluZWQgfTtcbiAgICAgICAgY29uc3QgaHR0cFJlZGlyZWN0ID0ge1xuICAgICAgICAgICAgaW5jb2duaXRvOiBib29sVG9JbnQodGFiLmluY29nbml0byksXG4gICAgICAgICAgICBicm93c2VyX2lkOiBjcmF3bElELFxuICAgICAgICAgICAgb2xkX3JlcXVlc3RfdXJsOiBlc2NhcGVVcmwoZGV0YWlscy51cmwpLFxuICAgICAgICAgICAgb2xkX3JlcXVlc3RfaWQ6IGRldGFpbHMucmVxdWVzdElkLFxuICAgICAgICAgICAgbmV3X3JlcXVlc3RfdXJsOiBlc2NhcGVVcmwoZGV0YWlscy5yZWRpcmVjdFVybCksXG4gICAgICAgICAgICBuZXdfcmVxdWVzdF9pZDogbnVsbCxcbiAgICAgICAgICAgIGV4dGVuc2lvbl9zZXNzaW9uX3V1aWQ6IGV4dGVuc2lvblNlc3Npb25VdWlkLFxuICAgICAgICAgICAgZXZlbnRfb3JkaW5hbDogZXZlbnRPcmRpbmFsLFxuICAgICAgICAgICAgd2luZG93X2lkOiB0YWIud2luZG93SWQsXG4gICAgICAgICAgICB0YWJfaWQ6IGRldGFpbHMudGFiSWQsXG4gICAgICAgICAgICBmcmFtZV9pZDogZGV0YWlscy5mcmFtZUlkLFxuICAgICAgICAgICAgcmVzcG9uc2Vfc3RhdHVzOiByZXNwb25zZVN0YXR1cyxcbiAgICAgICAgICAgIHJlc3BvbnNlX3N0YXR1c190ZXh0OiBlc2NhcGVTdHJpbmcocmVzcG9uc2VTdGF0dXNUZXh0KSxcbiAgICAgICAgICAgIGhlYWRlcnM6IHRoaXMuanNvbmlmeUhlYWRlcnMoZGV0YWlscy5yZXNwb25zZUhlYWRlcnMpLmhlYWRlcnMsXG4gICAgICAgICAgICB0aW1lX3N0YW1wOiBuZXcgRGF0ZShkZXRhaWxzLnRpbWVTdGFtcCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgfTtcbiAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIuc2F2ZVJlY29yZChcImh0dHBfcmVkaXJlY3RzXCIsIGh0dHBSZWRpcmVjdCk7XG4gICAgfVxuICAgIC8qXG4gICAgICogSFRUUCBSZXNwb25zZSBIYW5kbGVycyBhbmQgSGVscGVyIEZ1bmN0aW9uc1xuICAgICAqL1xuICAgIGFzeW5jIGxvZ1dpdGhSZXNwb25zZUJvZHkoZGV0YWlscywgdXBkYXRlKSB7XG4gICAgICAgIGNvbnN0IHBlbmRpbmdSZXNwb25zZSA9IHRoaXMuZ2V0UGVuZGluZ1Jlc3BvbnNlKGRldGFpbHMucmVxdWVzdElkKTtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgIGNvbnN0IHJlc3BvbnNlQm9keUxpc3RlbmVyID0gcGVuZGluZ1Jlc3BvbnNlLnJlc3BvbnNlQm9keUxpc3RlbmVyO1xuICAgICAgICAgICAgY29uc3QgcmVzcEJvZHkgPSBhd2FpdCByZXNwb25zZUJvZHlMaXN0ZW5lci5nZXRSZXNwb25zZUJvZHkoKTtcbiAgICAgICAgICAgIGNvbnN0IGNvbnRlbnRIYXNoID0gYXdhaXQgcmVzcG9uc2VCb2R5TGlzdGVuZXIuZ2V0Q29udGVudEhhc2goKTtcbiAgICAgICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLnNhdmVDb250ZW50KHJlc3BCb2R5LCBlc2NhcGVTdHJpbmcoY29udGVudEhhc2gpKTtcbiAgICAgICAgICAgIHVwZGF0ZS5jb250ZW50X2hhc2ggPSBjb250ZW50SGFzaDtcbiAgICAgICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLnNhdmVSZWNvcmQoXCJodHRwX3Jlc3BvbnNlc1wiLCB1cGRhdGUpO1xuICAgICAgICB9XG4gICAgICAgIGNhdGNoIChlcnIpIHtcbiAgICAgICAgICAgIC8qXG4gICAgICAgICAgICAvLyBUT0RPOiBSZWZhY3RvciB0byBjb3JyZXNwb25kaW5nIHdlYmV4dCBsb2dpYyBvciBkaXNjYXJkXG4gICAgICAgICAgICBkYXRhUmVjZWl2ZXIubG9nRXJyb3IoXG4gICAgICAgICAgICAgIFwiVW5hYmxlIHRvIHJldHJpZXZlIHJlc3BvbnNlIGJvZHkuXCIgKyBKU09OLnN0cmluZ2lmeShhUmVhc29uKSxcbiAgICAgICAgICAgICk7XG4gICAgICAgICAgICB1cGRhdGUuY29udGVudF9oYXNoID0gXCI8ZXJyb3I+XCI7XG4gICAgICAgICAgICBkYXRhUmVjZWl2ZXIuc2F2ZVJlY29yZChcImh0dHBfcmVzcG9uc2VzXCIsIHVwZGF0ZSk7XG4gICAgICAgICAgICAqL1xuICAgICAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIubG9nRXJyb3IoXCJVbmFibGUgdG8gcmV0cmlldmUgcmVzcG9uc2UgYm9keS5cIiArXG4gICAgICAgICAgICAgICAgXCJMaWtlbHkgY2F1c2VkIGJ5IGEgcHJvZ3JhbW1pbmcgZXJyb3IuIEVycm9yIE1lc3NhZ2U6XCIgK1xuICAgICAgICAgICAgICAgIGVyci5uYW1lICtcbiAgICAgICAgICAgICAgICBlcnIubWVzc2FnZSArXG4gICAgICAgICAgICAgICAgXCJcXG5cIiArXG4gICAgICAgICAgICAgICAgZXJyLnN0YWNrKTtcbiAgICAgICAgICAgIHVwZGF0ZS5jb250ZW50X2hhc2ggPSBcIjxlcnJvcj5cIjtcbiAgICAgICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLnNhdmVSZWNvcmQoXCJodHRwX3Jlc3BvbnNlc1wiLCB1cGRhdGUpO1xuICAgICAgICB9XG4gICAgfVxuICAgIC8vIEluc3RydW1lbnQgSFRUUCByZXNwb25zZXNcbiAgICBhc3luYyBvbkNvbXBsZXRlZEhhbmRsZXIoZGV0YWlscywgY3Jhd2xJRCwgZXZlbnRPcmRpbmFsLCBzYXZlQ29udGVudCkge1xuICAgICAgICAvKlxuICAgICAgICBjb25zb2xlLmxvZyhcbiAgICAgICAgICBcIm9uQ29tcGxldGVkSGFuZGxlciAocHJldmlvdXNseSBodHRwUmVxdWVzdEhhbmRsZXIpXCIsXG4gICAgICAgICAgZGV0YWlscyxcbiAgICAgICAgICBjcmF3bElELFxuICAgICAgICAgIHNhdmVDb250ZW50LFxuICAgICAgICApO1xuICAgICAgICAqL1xuICAgICAgICBjb25zdCB0YWIgPSBkZXRhaWxzLnRhYklkID4gLTFcbiAgICAgICAgICAgID8gYXdhaXQgYnJvd3Nlci50YWJzLmdldChkZXRhaWxzLnRhYklkKVxuICAgICAgICAgICAgOiB7IHdpbmRvd0lkOiB1bmRlZmluZWQsIGluY29nbml0bzogdW5kZWZpbmVkIH07XG4gICAgICAgIGNvbnN0IHVwZGF0ZSA9IHt9O1xuICAgICAgICB1cGRhdGUuaW5jb2duaXRvID0gYm9vbFRvSW50KHRhYi5pbmNvZ25pdG8pO1xuICAgICAgICB1cGRhdGUuYnJvd3Nlcl9pZCA9IGNyYXdsSUQ7XG4gICAgICAgIHVwZGF0ZS5leHRlbnNpb25fc2Vzc2lvbl91dWlkID0gZXh0ZW5zaW9uU2Vzc2lvblV1aWQ7XG4gICAgICAgIHVwZGF0ZS5ldmVudF9vcmRpbmFsID0gZXZlbnRPcmRpbmFsO1xuICAgICAgICB1cGRhdGUud2luZG93X2lkID0gdGFiLndpbmRvd0lkO1xuICAgICAgICB1cGRhdGUudGFiX2lkID0gZGV0YWlscy50YWJJZDtcbiAgICAgICAgdXBkYXRlLmZyYW1lX2lkID0gZGV0YWlscy5mcmFtZUlkO1xuICAgICAgICAvLyByZXF1ZXN0SWQgaXMgYSB1bmlxdWUgaWRlbnRpZmllciB0aGF0IGNhbiBiZSB1c2VkIHRvIGxpbmsgcmVxdWVzdHMgYW5kIHJlc3BvbnNlc1xuICAgICAgICB1cGRhdGUucmVxdWVzdF9pZCA9IE51bWJlcihkZXRhaWxzLnJlcXVlc3RJZCk7XG4gICAgICAgIGNvbnN0IGlzQ2FjaGVkID0gZGV0YWlscy5mcm9tQ2FjaGU7XG4gICAgICAgIHVwZGF0ZS5pc19jYWNoZWQgPSBib29sVG9JbnQoaXNDYWNoZWQpO1xuICAgICAgICBjb25zdCB1cmwgPSBkZXRhaWxzLnVybDtcbiAgICAgICAgdXBkYXRlLnVybCA9IGVzY2FwZVVybCh1cmwpO1xuICAgICAgICBjb25zdCByZXF1ZXN0TWV0aG9kID0gZGV0YWlscy5tZXRob2Q7XG4gICAgICAgIHVwZGF0ZS5tZXRob2QgPSBlc2NhcGVTdHJpbmcocmVxdWVzdE1ldGhvZCk7XG4gICAgICAgIC8vIFRPRE86IFJlZmFjdG9yIHRvIGNvcnJlc3BvbmRpbmcgd2ViZXh0IGxvZ2ljIG9yIGRpc2NhcmRcbiAgICAgICAgLy8gKHJlcXVlc3QgaGVhZGVycyBhcmUgbm90IGF2YWlsYWJsZSBpbiBodHRwIHJlc3BvbnNlIGV2ZW50IGxpc3RlbmVyIG9iamVjdCxcbiAgICAgICAgLy8gYnV0IHRoZSByZWZlcnJlciBwcm9wZXJ0eSBvZiB0aGUgY29ycmVzcG9uZGluZyByZXF1ZXN0IGNvdWxkIGJlIHF1ZXJpZWQpXG4gICAgICAgIC8vXG4gICAgICAgIC8vIGxldCByZWZlcnJlciA9IFwiXCI7XG4gICAgICAgIC8vIGlmIChkZXRhaWxzLnJlZmVycmVyKSB7XG4gICAgICAgIC8vICAgcmVmZXJyZXIgPSBkZXRhaWxzLnJlZmVycmVyLnNwZWM7XG4gICAgICAgIC8vIH1cbiAgICAgICAgLy8gdXBkYXRlLnJlZmVycmVyID0gZXNjYXBlU3RyaW5nKHJlZmVycmVyKTtcbiAgICAgICAgY29uc3QgcmVzcG9uc2VTdGF0dXMgPSBkZXRhaWxzLnN0YXR1c0NvZGU7XG4gICAgICAgIHVwZGF0ZS5yZXNwb25zZV9zdGF0dXMgPSByZXNwb25zZVN0YXR1cztcbiAgICAgICAgY29uc3QgcmVzcG9uc2VTdGF0dXNUZXh0ID0gZGV0YWlscy5zdGF0dXNMaW5lO1xuICAgICAgICB1cGRhdGUucmVzcG9uc2Vfc3RhdHVzX3RleHQgPSBlc2NhcGVTdHJpbmcocmVzcG9uc2VTdGF0dXNUZXh0KTtcbiAgICAgICAgY29uc3QgY3VycmVudF90aW1lID0gbmV3IERhdGUoZGV0YWlscy50aW1lU3RhbXApO1xuICAgICAgICB1cGRhdGUudGltZV9zdGFtcCA9IGN1cnJlbnRfdGltZS50b0lTT1N0cmluZygpO1xuICAgICAgICBjb25zdCBwYXJzZWRIZWFkZXJzID0gdGhpcy5qc29uaWZ5SGVhZGVycyhkZXRhaWxzLnJlc3BvbnNlSGVhZGVycyk7XG4gICAgICAgIHVwZGF0ZS5oZWFkZXJzID0gcGFyc2VkSGVhZGVycy5oZWFkZXJzO1xuICAgICAgICB1cGRhdGUubG9jYXRpb24gPSBwYXJzZWRIZWFkZXJzLmxvY2F0aW9uO1xuICAgICAgICBpZiAodGhpcy5zaG91bGRTYXZlQ29udGVudChzYXZlQ29udGVudCwgZGV0YWlscy50eXBlKSkge1xuICAgICAgICAgICAgdGhpcy5sb2dXaXRoUmVzcG9uc2VCb2R5KGRldGFpbHMsIHVwZGF0ZSk7XG4gICAgICAgIH1cbiAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICB0aGlzLmRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwiaHR0cF9yZXNwb25zZXNcIiwgdXBkYXRlKTtcbiAgICAgICAgfVxuICAgIH1cbiAgICBqc29uaWZ5SGVhZGVycyhoZWFkZXJzKSB7XG4gICAgICAgIGNvbnN0IHJlc3VsdEhlYWRlcnMgPSBbXTtcbiAgICAgICAgbGV0IGxvY2F0aW9uID0gXCJcIjtcbiAgICAgICAgaWYgKGhlYWRlcnMpIHtcbiAgICAgICAgICAgIGhlYWRlcnMubWFwKChyZXNwb25zZUhlYWRlcikgPT4ge1xuICAgICAgICAgICAgICAgIGNvbnN0IHsgbmFtZSwgdmFsdWUgfSA9IHJlc3BvbnNlSGVhZGVyO1xuICAgICAgICAgICAgICAgIGNvbnN0IGhlYWRlcl9wYWlyID0gW107XG4gICAgICAgICAgICAgICAgaGVhZGVyX3BhaXIucHVzaChlc2NhcGVTdHJpbmcobmFtZSkpO1xuICAgICAgICAgICAgICAgIGhlYWRlcl9wYWlyLnB1c2goZXNjYXBlU3RyaW5nKHZhbHVlKSk7XG4gICAgICAgICAgICAgICAgcmVzdWx0SGVhZGVycy5wdXNoKGhlYWRlcl9wYWlyKTtcbiAgICAgICAgICAgICAgICBpZiAobmFtZS50b0xvd2VyQ2FzZSgpID09PSBcImxvY2F0aW9uXCIpIHtcbiAgICAgICAgICAgICAgICAgICAgbG9jYXRpb24gPSB2YWx1ZTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4ge1xuICAgICAgICAgICAgaGVhZGVyczogSlNPTi5zdHJpbmdpZnkocmVzdWx0SGVhZGVycyksXG4gICAgICAgICAgICBsb2NhdGlvbjogZXNjYXBlU3RyaW5nKGxvY2F0aW9uKSxcbiAgICAgICAgfTtcbiAgICB9XG59XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2lhSFIwY0MxcGJuTjBjblZ0Wlc1MExtcHpJaXdpYzI5MWNtTmxVbTl2ZENJNklpSXNJbk52ZFhKalpYTWlPbHNpTGk0dkxpNHZMaTR2YzNKakwySmhZMnRuY205MWJtUXZhSFIwY0MxcGJuTjBjblZ0Wlc1MExuUnpJbDBzSW01aGJXVnpJanBiWFN3aWJXRndjR2x1WjNNaU9pSkJRVUZCTEU5QlFVOHNSVUZCUlN4MVFrRkJkVUlzUlVGQlJTeE5RVUZOTEhkRFFVRjNReXhEUVVGRE8wRkJRMnBHTEU5QlFVOHNSVUZCUlN4dlFrRkJiMElzUlVGQlJTeE5RVUZOTEN0Q1FVRXJRaXhEUVVGRE8wRkJRM0pGTEU5QlFVOHNSVUZCUlN4alFVRmpMRVZCUVhGQ0xFMUJRVTBzZVVKQlFYbENMRU5CUVVNN1FVRkROVVVzVDBGQlR5eEZRVUZGTEdOQlFXTXNSVUZCUlN4TlFVRk5MSGRDUVVGM1FpeERRVUZETzBGQlEzaEVMRTlCUVU4c1JVRkJSU3hsUVVGbExFVkJRVVVzVFVGQlRTeDVRa0ZCZVVJc1EwRkJRenRCUVVNeFJDeFBRVUZQTEVWQlFVVXNVMEZCVXl4RlFVRkZMRmxCUVZrc1JVRkJSU3hUUVVGVExFVkJRVVVzVFVGQlRTeHhRa0ZCY1VJc1EwRkJRenRCUVdWNlJUczdPenM3TzBkQlRVYzdRVUZGU0N4TlFVRk5MRkZCUVZFc1IwRkJiVUk3U1VGREwwSXNVVUZCVVR0SlFVTlNMRmxCUVZrN1NVRkRXaXhOUVVGTk8wbEJRMDRzVDBGQlR6dEpRVU5RTEZWQlFWVTdTVUZEVml4WlFVRlpPMGxCUTFvc1QwRkJUenRKUVVOUUxGRkJRVkU3U1VGRFVpeHRRa0ZCYlVJN1NVRkRia0lzVFVGQlRUdEpRVU5PTEZGQlFWRTdTVUZEVWl4aFFVRmhPMGxCUTJJc1dVRkJXVHRKUVVOYUxGZEJRVmM3U1VGRFdDeGpRVUZqTzBsQlEyUXNWMEZCVnp0SlFVTllMRk5CUVZNN1NVRkRWQ3huUWtGQlowSTdTVUZEYUVJc1RVRkJUVHRKUVVOT0xFOUJRVTg3UTBGRFVpeERRVUZETzBGQlJVWXNUMEZCVHl4RlFVRkZMRkZCUVZFc1JVRkJSU3hEUVVGRE8wRkJSWEJDTEUxQlFVMHNUMEZCVHl4alFVRmpPMGxCUTFJc1dVRkJXU3hEUVVGRE8wbEJRM1JDTEdWQlFXVXNSMEZGYmtJc1JVRkJSU3hEUVVGRE8wbEJRME1zWjBKQlFXZENMRWRCUlhCQ0xFVkJRVVVzUTBGQlF6dEpRVU5ETEhWQ1FVRjFRaXhEUVVGRE8wbEJRM2hDTERKQ1FVRXlRaXhEUVVGRE8wbEJRelZDTEhkQ1FVRjNRaXhEUVVGRE8wbEJRM3BDTEcxQ1FVRnRRaXhEUVVGRE8wbEJSVFZDTEZsQlFWa3NXVUZCV1R0UlFVTjBRaXhKUVVGSkxFTkJRVU1zV1VGQldTeEhRVUZITEZsQlFWa3NRMEZCUXp0SlFVTnVReXhEUVVGRE8wbEJSVTBzUjBGQlJ5eERRVUZETEU5QlFVOHNSVUZCUlN4cFFrRkJiME03VVVGRGRFUXNUVUZCVFN4TlFVRk5MRWRCUVd0Q0xFVkJRVVVzU1VGQlNTeEZRVUZGTEVOQlFVTXNXVUZCV1N4RFFVRkRMRVZCUVVVc1MwRkJTeXhGUVVGRkxGRkJRVkVzUlVGQlJTeERRVUZETzFGQlJYaEZMRTFCUVUwc2VVSkJRWGxDTEVkQlFVY3NRMEZCUXl4UFFVRlBMRVZCUVVVc1JVRkJSVHRaUVVNMVF5eFBRVUZQTEVOQlEwd3NUMEZCVHl4RFFVRkRMRk5CUVZNc1NVRkJTU3hQUVVGUExFTkJRVU1zVTBGQlV5eERRVUZETEU5QlFVOHNRMEZCUXl4clFrRkJhMElzUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVTjRSU3hEUVVGRE8xRkJRMG9zUTBGQlF5eERRVUZETzFGQlJVWTdPMWRCUlVjN1VVRkZTQ3hKUVVGSkxFTkJRVU1zZFVKQlFYVkNMRWRCUVVjc1EwRkROMElzVDBGQk9FTXNSVUZET1VNc1JVRkJSVHRaUVVOR0xFMUJRVTBzSzBKQlFTdENMRWRCUVhGQ0xFVkJRVVVzUTBGQlF6dFpRVU0zUkN4eFEwRkJjVU03V1VGRGNrTXNTVUZCU1N4NVFrRkJlVUlzUTBGQlF5eFBRVUZQTEVOQlFVTXNSVUZCUlR0blFrRkRkRU1zVDBGQlR5d3JRa0ZCSzBJc1EwRkJRenRoUVVONFF6dFpRVU5FTEUxQlFVMHNZMEZCWXl4SFFVRkhMRWxCUVVrc1EwRkJReXhwUWtGQmFVSXNRMEZCUXl4UFFVRlBMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU03V1VGRGFrVXNZMEZCWXl4RFFVRkRMR3REUVVGclF5eERRVUZETEU5QlFVOHNRMEZCUXl4RFFVRkRPMWxCUXpORUxFMUJRVTBzWlVGQlpTeEhRVUZITEVsQlFVa3NRMEZCUXl4clFrRkJhMElzUTBGQlF5eFBRVUZQTEVOQlFVTXNVMEZCVXl4RFFVRkRMRU5CUVVNN1dVRkRia1VzWlVGQlpTeERRVUZETEd0RFFVRnJReXhEUVVGRExFOUJRVThzUTBGQlF5eERRVUZETzFsQlF6VkVMRWxCUVVrc1NVRkJTU3hEUVVGRExHbENRVUZwUWl4RFFVRkRMR2xDUVVGcFFpeEZRVUZGTEU5QlFVOHNRMEZCUXl4SlFVRkpMRU5CUVVNc1JVRkJSVHRuUWtGRE0wUXNaVUZCWlN4RFFVRkRMQ3RDUVVFclFpeERRVUZETEU5QlFVOHNRMEZCUXl4RFFVRkRPMkZCUXpGRU8xbEJRMFFzVDBGQlR5d3JRa0ZCSzBJc1EwRkJRenRSUVVONlF5eERRVUZETEVOQlFVTTdVVUZEUml4UFFVRlBMRU5CUVVNc1ZVRkJWU3hEUVVGRExHVkJRV1VzUTBGQlF5eFhRVUZYTEVOQlF6VkRMRWxCUVVrc1EwRkJReXgxUWtGQmRVSXNSVUZETlVJc1RVRkJUU3hGUVVOT0xFbEJRVWtzUTBGQlF5eHpRa0ZCYzBJc1EwRkJReXhwUWtGQmFVSXNRMEZCUXp0WlFVTTFReXhEUVVGRExFTkJRVU1zUTBGQlF5eGhRVUZoTEVWQlFVVXNWVUZCVlN4RFFVRkRPMWxCUXpkQ0xFTkJRVU1zUTBGQlF5eERRVUZETEdGQlFXRXNRMEZCUXl4RFFVTndRaXhEUVVGRE8xRkJSVVlzU1VGQlNTeERRVUZETERKQ1FVRXlRaXhIUVVGSExFTkJRVU1zVDBGQlR5eEZRVUZGTEVWQlFVVTdXVUZETjBNc2NVTkJRWEZETzFsQlEzSkRMRWxCUVVrc2VVSkJRWGxDTEVOQlFVTXNUMEZCVHl4RFFVRkRMRVZCUVVVN1owSkJRM1JETEU5QlFVODdZVUZEVWp0WlFVTkVMRTFCUVUwc1kwRkJZeXhIUVVGSExFbEJRVWtzUTBGQlF5eHBRa0ZCYVVJc1EwRkJReXhQUVVGUExFTkJRVU1zVTBGQlV5eERRVUZETEVOQlFVTTdXVUZEYWtVc1kwRkJZeXhEUVVGRExITkRRVUZ6UXl4RFFVRkRMRTlCUVU4c1EwRkJReXhEUVVGRE8xbEJReTlFTEVsQlFVa3NRMEZCUXl3d1FrRkJNRUlzUTBGRE4wSXNUMEZCVHl4RlFVTlFMRTlCUVU4c1JVRkRVQ3gxUWtGQmRVSXNSVUZCUlN4RFFVTXhRaXhEUVVGRE8xRkJRMG9zUTBGQlF5eERRVUZETzFGQlEwWXNUMEZCVHl4RFFVRkRMRlZCUVZVc1EwRkJReXh0UWtGQmJVSXNRMEZCUXl4WFFVRlhMRU5CUTJoRUxFbEJRVWtzUTBGQlF5d3lRa0ZCTWtJc1JVRkRhRU1zVFVGQlRTeEZRVU5PTEVOQlFVTXNaMEpCUVdkQ0xFTkJRVU1zUTBGRGJrSXNRMEZCUXp0UlFVVkdMRWxCUVVrc1EwRkJReXgzUWtGQmQwSXNSMEZCUnl4RFFVRkRMRTlCUVU4c1JVRkJSU3hGUVVGRk8xbEJRekZETEhGRFFVRnhRenRaUVVOeVF5eEpRVUZKTEhsQ1FVRjVRaXhEUVVGRExFOUJRVThzUTBGQlF5eEZRVUZGTzJkQ1FVTjBReXhQUVVGUE8yRkJRMUk3V1VGRFJDeEpRVUZKTEVOQlFVTXNkVUpCUVhWQ0xFTkJRVU1zVDBGQlR5eEZRVUZGTEU5QlFVOHNSVUZCUlN4MVFrRkJkVUlzUlVGQlJTeERRVUZETEVOQlFVTTdVVUZETlVVc1EwRkJReXhEUVVGRE8xRkJRMFlzVDBGQlR5eERRVUZETEZWQlFWVXNRMEZCUXl4blFrRkJaMElzUTBGQlF5eFhRVUZYTEVOQlF6ZERMRWxCUVVrc1EwRkJReXgzUWtGQmQwSXNSVUZETjBJc1RVRkJUU3hGUVVOT0xFTkJRVU1zYVVKQlFXbENMRU5CUVVNc1EwRkRjRUlzUTBGQlF6dFJRVVZHTEVsQlFVa3NRMEZCUXl4dFFrRkJiVUlzUjBGQlJ5eERRVUZETEU5QlFVOHNSVUZCUlN4RlFVRkZPMWxCUTNKRExIRkRRVUZ4UXp0WlFVTnlReXhKUVVGSkxIbENRVUY1UWl4RFFVRkRMRTlCUVU4c1EwRkJReXhGUVVGRk8yZENRVU4wUXl4UFFVRlBPMkZCUTFJN1dVRkRSQ3hOUVVGTkxHVkJRV1VzUjBGQlJ5eEpRVUZKTEVOQlFVTXNhMEpCUVd0Q0xFTkJRVU1zVDBGQlR5eERRVUZETEZOQlFWTXNRMEZCUXl4RFFVRkRPMWxCUTI1RkxHVkJRV1VzUTBGQlF5dzRRa0ZCT0VJc1EwRkJReXhQUVVGUExFTkJRVU1zUTBGQlF6dFpRVU40UkN4SlFVRkpMRU5CUVVNc2EwSkJRV3RDTEVOQlEzSkNMRTlCUVU4c1JVRkRVQ3hQUVVGUExFVkJRMUFzZFVKQlFYVkNMRVZCUVVVc1JVRkRla0lzYVVKQlFXbENMRU5CUTJ4Q0xFTkJRVU03VVVGRFNpeERRVUZETEVOQlFVTTdVVUZEUml4UFFVRlBMRU5CUVVNc1ZVRkJWU3hEUVVGRExGZEJRVmNzUTBGQlF5eFhRVUZYTEVOQlEzaERMRWxCUVVrc1EwRkJReXh0UWtGQmJVSXNSVUZEZUVJc1RVRkJUU3hGUVVOT0xFTkJRVU1zYVVKQlFXbENMRU5CUVVNc1EwRkRjRUlzUTBGQlF6dEpRVU5LTEVOQlFVTTdTVUZGVFN4UFFVRlBPMUZCUTFvc1NVRkJTU3hKUVVGSkxFTkJRVU1zZFVKQlFYVkNMRVZCUVVVN1dVRkRhRU1zVDBGQlR5eERRVUZETEZWQlFWVXNRMEZCUXl4bFFVRmxMRU5CUVVNc1kwRkJZeXhEUVVNdlF5eEpRVUZKTEVOQlFVTXNkVUpCUVhWQ0xFTkJRemRDTEVOQlFVTTdVMEZEU0R0UlFVTkVMRWxCUVVrc1NVRkJTU3hEUVVGRExESkNRVUV5UWl4RlFVRkZPMWxCUTNCRExFOUJRVThzUTBGQlF5eFZRVUZWTEVOQlFVTXNiVUpCUVcxQ0xFTkJRVU1zWTBGQll5eERRVU51UkN4SlFVRkpMRU5CUVVNc01rSkJRVEpDTEVOQlEycERMRU5CUVVNN1UwRkRTRHRSUVVORUxFbEJRVWtzU1VGQlNTeERRVUZETEhkQ1FVRjNRaXhGUVVGRk8xbEJRMnBETEU5QlFVOHNRMEZCUXl4VlFVRlZMRU5CUVVNc1owSkJRV2RDTEVOQlFVTXNZMEZCWXl4RFFVTm9SQ3hKUVVGSkxFTkJRVU1zZDBKQlFYZENMRU5CUXpsQ0xFTkJRVU03VTBGRFNEdFJRVU5FTEVsQlFVa3NTVUZCU1N4RFFVRkRMRzFDUVVGdFFpeEZRVUZGTzFsQlF6VkNMRTlCUVU4c1EwRkJReXhWUVVGVkxFTkJRVU1zVjBGQlZ5eERRVUZETEdOQlFXTXNRMEZCUXl4SlFVRkpMRU5CUVVNc2JVSkJRVzFDTEVOQlFVTXNRMEZCUXp0VFFVTjZSVHRKUVVOSUxFTkJRVU03U1VGRlR5eHpRa0ZCYzBJc1EwRkJReXhwUWtGQmIwTTdVVUZEYWtVc1NVRkJTU3hwUWtGQmFVSXNTMEZCU3l4SlFVRkpMRVZCUVVVN1dVRkRPVUlzVDBGQlR5eEpRVUZKTEVOQlFVTTdVMEZEWWp0UlFVTkVMRWxCUVVrc2FVSkJRV2xDTEV0QlFVc3NTMEZCU3l4RlFVRkZPMWxCUXk5Q0xFOUJRVThzUzBGQlN5eERRVUZETzFOQlEyUTdVVUZEUkN4UFFVRlBMRWxCUVVrc1EwRkJReXgzUWtGQmQwSXNRMEZCUXl4cFFrRkJhVUlzUTBGQlF5eERRVUZETEUxQlFVMHNSMEZCUnl4RFFVRkRMRU5CUVVNN1NVRkRja1VzUTBGQlF6dEpRVVZQTEhkQ1FVRjNRaXhEUVVGRExHbENRVUY1UWp0UlFVTjRSQ3hQUVVGUExHbENRVUZwUWl4RFFVRkRMRXRCUVVzc1EwRkJReXhIUVVGSExFTkJRVzFDTEVOQlFVTTdTVUZEZUVRc1EwRkJRenRKUVVWRU96czdPenM3VDBGTlJ6dEpRVU5MTEdsQ1FVRnBRaXhEUVVOMlFpeHBRa0ZCYjBNc1JVRkRjRU1zV1VGQk1FSTdVVUZGTVVJc1NVRkJTU3hwUWtGQmFVSXNTMEZCU3l4SlFVRkpMRVZCUVVVN1dVRkRPVUlzVDBGQlR5eEpRVUZKTEVOQlFVTTdVMEZEWWp0UlFVTkVMRWxCUVVrc2FVSkJRV2xDTEV0QlFVc3NTMEZCU3l4RlFVRkZPMWxCUXk5Q0xFOUJRVThzUzBGQlN5eERRVUZETzFOQlEyUTdVVUZEUkN4UFFVRlBMRWxCUVVrc1EwRkJReXgzUWtGQmQwSXNRMEZCUXl4cFFrRkJhVUlzUTBGQlF5eERRVUZETEZGQlFWRXNRMEZET1VRc1dVRkJXU3hEUVVOaUxFTkJRVU03U1VGRFNpeERRVUZETzBsQlJVOHNhVUpCUVdsQ0xFTkJRVU1zVTBGQlV6dFJRVU5xUXl4SlFVRkpMRU5CUVVNc1NVRkJTU3hEUVVGRExHVkJRV1VzUTBGQlF5eFRRVUZUTEVOQlFVTXNSVUZCUlR0WlFVTndReXhKUVVGSkxFTkJRVU1zWlVGQlpTeERRVUZETEZOQlFWTXNRMEZCUXl4SFFVRkhMRWxCUVVrc1kwRkJZeXhGUVVGRkxFTkJRVU03VTBGRGVFUTdVVUZEUkN4UFFVRlBMRWxCUVVrc1EwRkJReXhsUVVGbExFTkJRVU1zVTBGQlV5eERRVUZETEVOQlFVTTdTVUZEZWtNc1EwRkJRenRKUVVWUExHdENRVUZyUWl4RFFVRkRMRk5CUVZNN1VVRkRiRU1zU1VGQlNTeERRVUZETEVsQlFVa3NRMEZCUXl4blFrRkJaMElzUTBGQlF5eFRRVUZUTEVOQlFVTXNSVUZCUlR0WlFVTnlReXhKUVVGSkxFTkJRVU1zWjBKQlFXZENMRU5CUVVNc1UwRkJVeXhEUVVGRExFZEJRVWNzU1VGQlNTeGxRVUZsTEVWQlFVVXNRMEZCUXp0VFFVTXhSRHRSUVVORUxFOUJRVThzU1VGQlNTeERRVUZETEdkQ1FVRm5RaXhEUVVGRExGTkJRVk1zUTBGQlF5eERRVUZETzBsQlF6RkRMRU5CUVVNN1NVRkZSRHM3VDBGRlJ6dEpRVVZMTEV0QlFVc3NRMEZCUXl3d1FrRkJNRUlzUTBGRGRFTXNUMEZCYTBRc1JVRkRiRVFzVDBGQlR5eEZRVU5RTEZsQlFXOUNPMUZCUlhCQ0xFMUJRVTBzUjBGQlJ5eEhRVU5RTEU5QlFVOHNRMEZCUXl4TFFVRkxMRWRCUVVjc1EwRkJReXhEUVVGRE8xbEJRMmhDTEVOQlFVTXNRMEZCUXl4TlFVRk5MRTlCUVU4c1EwRkJReXhKUVVGSkxFTkJRVU1zUjBGQlJ5eERRVUZETEU5QlFVOHNRMEZCUXl4TFFVRkxMRU5CUVVNN1dVRkRka01zUTBGQlF5eERRVUZETEVWQlFVVXNVVUZCVVN4RlFVRkZMRk5CUVZNc1JVRkJSU3hUUVVGVExFVkJRVVVzVTBGQlV5eEZRVUZGTEVkQlFVY3NSVUZCUlN4VFFVRlRMRVZCUVVVc1EwRkJRenRSUVVWd1JTeE5RVUZOTEUxQlFVMHNSMEZCUnl4RlFVRnBRaXhEUVVGRE8xRkJSV3BETEUxQlFVMHNRMEZCUXl4VFFVRlRMRWRCUVVjc1UwRkJVeXhEUVVGRExFZEJRVWNzUTBGQlF5eFRRVUZUTEVOQlFVTXNRMEZCUXp0UlFVTTFReXhOUVVGTkxFTkJRVU1zVlVGQlZTeEhRVUZITEU5QlFVOHNRMEZCUXp0UlFVTTFRaXhOUVVGTkxFTkJRVU1zYzBKQlFYTkNMRWRCUVVjc2IwSkJRVzlDTEVOQlFVTTdVVUZEY2tRc1RVRkJUU3hEUVVGRExHRkJRV0VzUjBGQlJ5eFpRVUZaTEVOQlFVTTdVVUZEY0VNc1RVRkJUU3hEUVVGRExGTkJRVk1zUjBGQlJ5eEhRVUZITEVOQlFVTXNVVUZCVVN4RFFVRkRPMUZCUTJoRExFMUJRVTBzUTBGQlF5eE5RVUZOTEVkQlFVY3NUMEZCVHl4RFFVRkRMRXRCUVVzc1EwRkJRenRSUVVNNVFpeE5RVUZOTEVOQlFVTXNVVUZCVVN4SFFVRkhMRTlCUVU4c1EwRkJReXhQUVVGUExFTkJRVU03VVVGRmJFTXNiVVpCUVcxR08xRkJRMjVHTEUxQlFVMHNRMEZCUXl4VlFVRlZMRWRCUVVjc1RVRkJUU3hEUVVGRExFOUJRVThzUTBGQlF5eFRRVUZUTEVOQlFVTXNRMEZCUXp0UlFVVTVReXhOUVVGTkxFZEJRVWNzUjBGQlJ5eFBRVUZQTEVOQlFVTXNSMEZCUnl4RFFVRkRPMUZCUTNoQ0xFMUJRVTBzUTBGQlF5eEhRVUZITEVkQlFVY3NVMEZCVXl4RFFVRkRMRWRCUVVjc1EwRkJReXhEUVVGRE8xRkJSVFZDTEUxQlFVMHNZVUZCWVN4SFFVRkhMRTlCUVU4c1EwRkJReXhOUVVGTkxFTkJRVU03VVVGRGNrTXNUVUZCVFN4RFFVRkRMRTFCUVUwc1IwRkJSeXhaUVVGWkxFTkJRVU1zWVVGQllTeERRVUZETEVOQlFVTTdVVUZGTlVNc1RVRkJUU3haUVVGWkxFZEJRVWNzU1VGQlNTeEpRVUZKTEVOQlFVTXNUMEZCVHl4RFFVRkRMRk5CUVZNc1EwRkJReXhEUVVGRE8xRkJRMnBFTEUxQlFVMHNRMEZCUXl4VlFVRlZMRWRCUVVjc1dVRkJXU3hEUVVGRExGZEJRVmNzUlVGQlJTeERRVUZETzFGQlJTOURMRWxCUVVrc1dVRkJXU3hIUVVGSExFVkJRVVVzUTBGQlF6dFJRVU4wUWl4SlFVRkpMRkZCUVZFc1IwRkJSeXhGUVVGRkxFTkJRVU03VVVGRGJFSXNUVUZCVFN4UFFVRlBMRWRCUVVjc1JVRkJSU3hEUVVGRE8xRkJRMjVDTEVsQlFVa3NUVUZCVFN4SFFVRkhMRXRCUVVzc1EwRkJRenRSUVVOdVFpeEpRVUZKTEU5QlFVOHNRMEZCUXl4alFVRmpMRVZCUVVVN1dVRkRNVUlzVDBGQlR5eERRVUZETEdOQlFXTXNRMEZCUXl4SFFVRkhMRU5CUVVNc1EwRkJReXhoUVVGaExFVkJRVVVzUlVGQlJUdG5Ra0ZETTBNc1RVRkJUU3hGUVVGRkxFbEJRVWtzUlVGQlJTeExRVUZMTEVWQlFVVXNSMEZCUnl4aFFVRmhMRU5CUVVNN1owSkJRM1JETEUxQlFVMHNWMEZCVnl4SFFVRkhMRVZCUVVVc1EwRkJRenRuUWtGRGRrSXNWMEZCVnl4RFFVRkRMRWxCUVVrc1EwRkJReXhaUVVGWkxFTkJRVU1zU1VGQlNTeERRVUZETEVOQlFVTXNRMEZCUXp0blFrRkRja01zVjBGQlZ5eERRVUZETEVsQlFVa3NRMEZCUXl4WlFVRlpMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU1zUTBGQlF6dG5Ra0ZEZEVNc1QwRkJUeXhEUVVGRExFbEJRVWtzUTBGQlF5eFhRVUZYTEVOQlFVTXNRMEZCUXp0blFrRkRNVUlzU1VGQlNTeEpRVUZKTEV0QlFVc3NZMEZCWXl4RlFVRkZPMjlDUVVNelFpeFpRVUZaTEVkQlFVY3NTMEZCU3l4RFFVRkRPMjlDUVVOeVFpeEpRVUZKTEZsQlFWa3NRMEZCUXl4UFFVRlBMRU5CUVVNc01FSkJRVEJDTEVOQlFVTXNTMEZCU3l4RFFVRkRMRU5CUVVNc1JVRkJSVHQzUWtGRE0wUXNUVUZCVFN4SFFVRkhMRWxCUVVrc1EwRkJRenR4UWtGRFpqdHBRa0ZEUmp0blFrRkRSQ3hKUVVGSkxFbEJRVWtzUzBGQlN5eFRRVUZUTEVWQlFVVTdiMEpCUTNSQ0xGRkJRVkVzUjBGQlJ5eExRVUZMTEVOQlFVTTdhVUpCUTJ4Q08xbEJRMGdzUTBGQlF5eERRVUZETEVOQlFVTTdVMEZEU2p0UlFVVkVMRTFCUVUwc1EwRkJReXhSUVVGUkxFZEJRVWNzV1VGQldTeERRVUZETEZGQlFWRXNRMEZCUXl4RFFVRkRPMUZCUlhwRExFbEJRVWtzWVVGQllTeExRVUZMTEUxQlFVMHNTVUZCU1N4RFFVRkRMRTFCUVUwc1EwRkJReXhwUTBGQmFVTXNSVUZCUlR0WlFVTjZSU3hOUVVGTkxHTkJRV01zUjBGQlJ5eEpRVUZKTEVOQlFVTXNhVUpCUVdsQ0xFTkJRVU1zVDBGQlR5eERRVUZETEZOQlFWTXNRMEZCUXl4RFFVRkRPMWxCUTJwRkxFMUJRVTBzVVVGQlVTeEhRVUZITEUxQlFVMHNZMEZCWXl4RFFVRkRMSEZDUVVGeFFpeERRVUZETEVsQlFVa3NRMEZCUXl4RFFVRkRPMWxCUTJ4RkxFbEJRVWtzUTBGQlF5eFJRVUZSTEVWQlFVVTdaMEpCUTJJc1NVRkJTU3hEUVVGRExGbEJRVmtzUTBGQlF5eFJRVUZSTEVOQlEzaENMSEZIUVVGeFJ5eERRVU4wUnl4RFFVRkRPMkZCUTBnN2FVSkJRVTA3WjBKQlEwd3NUVUZCVFN3eVFrRkJNa0lzUjBGREwwSXNUVUZCVFN4alFVRmpMRU5CUVVNc01rSkJRVEpDTEVOQlFVTTdaMEpCUTI1RUxFMUJRVTBzVjBGQlZ5eEhRVUZITERKQ1FVRXlRaXhEUVVGRExGZEJRVmNzUTBGQlF6dG5Ra0ZGTlVRc1NVRkJTU3hYUVVGWExFVkJRVVU3YjBKQlEyWXNUVUZCVFN4VlFVRlZMRWRCUVVjc1NVRkJTU3hqUVVGakxFTkJRMjVETERKQ1FVRXlRaXhGUVVNelFpeEpRVUZKTEVOQlFVTXNXVUZCV1N4RFFVTnNRaXhEUVVGRE8yOUNRVU5HTEUxQlFVMHNUMEZCVHl4SFFVRnpRaXhWUVVGVkxFTkJRVU1zWjBKQlFXZENMRVZCUVVVc1EwRkJRenR2UWtGRmFrVXNaMFJCUVdkRU8yOUNRVU5vUkN4SlFVRkpMR05CUVdNc1NVRkJTU3hQUVVGUExFVkJRVVU3ZDBKQlF6ZENMREJHUVVFd1JqdDNRa0ZETVVZc2JVZEJRVzFITzNkQ1FVTnVSeXhOUVVGTkxHTkJRV01zUjBGQlJ6czBRa0ZEY2tJc1kwRkJZenMwUWtGRFpDeHhRa0ZCY1VJN05FSkJRM0pDTEdkQ1FVRm5RanQ1UWtGRGFrSXNRMEZCUXp0M1FrRkRSaXhMUVVGTExFMUJRVTBzU1VGQlNTeEpRVUZKTEU5QlFVOHNRMEZCUXl4WlFVRlpMRVZCUVVVN05FSkJRM1pETEVsQlFVa3NZMEZCWXl4RFFVRkRMRkZCUVZFc1EwRkJReXhKUVVGSkxFTkJRVU1zUlVGQlJUdG5RMEZEYWtNc1RVRkJUU3hYUVVGWExFZEJRVWNzUlVGQlJTeERRVUZETzJkRFFVTjJRaXhYUVVGWExFTkJRVU1zU1VGQlNTeERRVUZETEZsQlFWa3NRMEZCUXl4SlFVRkpMRU5CUVVNc1EwRkJReXhEUVVGRE8yZERRVU55UXl4WFFVRlhMRU5CUVVNc1NVRkJTU3hEUVVGRExGbEJRVmtzUTBGQlF5eFBRVUZQTEVOQlFVTXNXVUZCV1N4RFFVRkRMRWxCUVVrc1EwRkJReXhEUVVGRExFTkJRVU1zUTBGQlF6dG5RMEZETTBRc1QwRkJUeXhEUVVGRExFbEJRVWtzUTBGQlF5eFhRVUZYTEVOQlFVTXNRMEZCUXpzMlFrRkRNMEk3ZVVKQlEwWTdjVUpCUTBZN2IwSkJRMFFzSzBaQlFTdEdPMjlDUVVNdlJpeEpRVUZKTEZkQlFWY3NTVUZCU1N4UFFVRlBMRVZCUVVVN2QwSkJRekZDTEUxQlFVMHNRMEZCUXl4VFFVRlRMRWRCUVVjc1QwRkJUeXhEUVVGRExGTkJRVk1zUTBGQlF6dHhRa0ZEZEVNN2IwSkJRMFFzU1VGQlNTeGxRVUZsTEVsQlFVa3NUMEZCVHl4RlFVRkZPM2RDUVVNNVFpeE5RVUZOTEVOQlFVTXNZVUZCWVN4SFFVRkhMRTlCUVU4c1EwRkJReXhoUVVGaExFTkJRVU03Y1VKQlF6bERPMmxDUVVOR08yRkJRMFk3VTBGRFJqdFJRVVZFTEUxQlFVMHNRMEZCUXl4UFFVRlBMRWRCUVVjc1NVRkJTU3hEUVVGRExGTkJRVk1zUTBGQlF5eFBRVUZQTEVOQlFVTXNRMEZCUXp0UlFVVjZReXhsUVVGbE8xRkJRMllzVFVGQlRTeExRVUZMTEVkQlFVY3NUMEZCVHl4RFFVRkRMRWxCUVVrc1MwRkJTeXhuUWtGQlowSXNRMEZCUXp0UlFVTm9SQ3hOUVVGTkxFTkJRVU1zVFVGQlRTeEhRVUZITEZOQlFWTXNRMEZCUXl4TFFVRkxMRU5CUVVNc1EwRkJRenRSUVVWcVF5dzJRMEZCTmtNN1VVRkROME1zU1VGQlNTeG5Ra0ZCWjBJc1EwRkJRenRSUVVOeVFpeEpRVUZKTEdGQlFXRXNRMEZCUXp0UlFVTnNRaXhKUVVGSkxFOUJRVThzUTBGQlF5eFRRVUZUTEVWQlFVVTdXVUZEY2tJc1RVRkJUU3hsUVVGbExFZEJRVWNzU1VGQlNTeEhRVUZITEVOQlFVTXNUMEZCVHl4RFFVRkRMRk5CUVZNc1EwRkJReXhEUVVGRE8xbEJRMjVFTEdkQ1FVRm5RaXhIUVVGSExHVkJRV1VzUTBGQlF5eE5RVUZOTEVOQlFVTTdVMEZETTBNN1VVRkRSQ3hKUVVGSkxFOUJRVThzUTBGQlF5eFhRVUZYTEVWQlFVVTdXVUZEZGtJc1RVRkJUU3hwUWtGQmFVSXNSMEZCUnl4SlFVRkpMRWRCUVVjc1EwRkJReXhQUVVGUExFTkJRVU1zVjBGQlZ5eERRVUZETEVOQlFVTTdXVUZEZGtRc1lVRkJZU3hIUVVGSExHbENRVUZwUWl4RFFVRkRMRTFCUVUwc1EwRkJRenRUUVVNeFF6dFJRVU5FTEUxQlFVMHNRMEZCUXl4cFFrRkJhVUlzUjBGQlJ5eFpRVUZaTEVOQlFVTXNaMEpCUVdkQ0xFTkJRVU1zUTBGQlF6dFJRVU14UkN4TlFVRk5MRU5CUVVNc1kwRkJZeXhIUVVGSExGbEJRVmtzUTBGQlF5eGhRVUZoTEVOQlFVTXNRMEZCUXp0UlFVVndSQ3g1UWtGQmVVSTdVVUZEZWtJc2VVVkJRWGxGTzFGQlEzcEZMRGhDUVVFNFFqdFJRVU01UWl4TlFVRk5MRmRCUVZjc1IwRkJSeXhQUVVGUExFTkJRVU1zVjBGQlZ5eERRVUZETzFGQlEzaERMRTFCUVUwc1EwRkJReXhaUVVGWkxFZEJRVWNzV1VGQldTeERRVUZETEZkQlFWY3NRMEZCUXl4RFFVRkRPMUZCUldoRUxHdEZRVUZyUlR0UlFVTnNSU3hwUmtGQmFVWTdVVUZEYWtZc2FVSkJRV2xDTzFGQlEycENMSEZIUVVGeFJ6dFJRVU55Unl4TlFVRk5MRU5CUVVNc1lVRkJZU3hIUVVGSExFOUJRVThzUTBGQlF5eEpRVUZKTEVOQlFVTTdVVUZGY0VNN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenRWUVRCRFJUdFJRVU5HTEUxQlFVMHNRMEZCUXl4aFFVRmhMRWRCUVVjc1UwRkJVeXhEUVVGRExFbEJRVWtzUTBGQlF5eDNRa0ZCZDBJc1EwRkJReXhQUVVGUExFTkJRVU1zUTBGQlF5eERRVUZETzFGQlEzcEZMRTFCUVUwc1EwRkJReXhsUVVGbExFZEJRVWNzVDBGQlR5eERRVUZETEdGQlFXRXNRMEZCUXp0UlFVTXZReXhOUVVGTkxFTkJRVU1zWlVGQlpTeEhRVUZITEZsQlFWa3NRMEZEYmtNc1NVRkJTU3hEUVVGRExGTkJRVk1zUTBGQlF5eFBRVUZQTEVOQlFVTXNZMEZCWXl4RFFVRkRMRU5CUTNaRExFTkJRVU03VVVGRFJpeEpRVUZKTEVOQlFVTXNXVUZCV1N4RFFVRkRMRlZCUVZVc1EwRkJReXhsUVVGbExFVkJRVVVzVFVGQlRTeERRVUZETEVOQlFVTTdTVUZEZUVRc1EwRkJRenRKUVVWRU96czdPenM3T3pzN096czdUMEZaUnp0SlFVTkxMSGRDUVVGM1FpeERRVU01UWl4UFFVRnJSRHRSUVVWc1JDeEpRVUZKTEVkQlFVY3NSMEZCUnl4RlFVRkZMRU5CUVVNN1VVRkZZaXhKUVVGSkxFOUJRVThzUTBGQlF5eEpRVUZKTEV0QlFVc3NXVUZCV1N4RlFVRkZPMWxCUTJwRExIZERRVUYzUXp0WlFVTjRReXhIUVVGSExFZEJRVWNzVDBGQlR5eERRVUZETEVkQlFVY3NRMEZCUXp0VFFVTnVRanRoUVVGTkxFbEJRVWtzVDBGQlR5eERRVUZETEdOQlFXTXNRMEZCUXl4blFrRkJaMElzUTBGQlF5eEZRVUZGTzFsQlEyNUVMR2xGUVVGcFJUdFpRVU5xUlN4elJVRkJjMFU3V1VGRGRFVXNSMEZCUnl4SFFVRkhMRTlCUVU4c1EwRkJReXhqUVVGakxFTkJRVU1zVFVGQlRUdG5Ra0ZEYWtNc1EwRkJReXhEUVVGRExFOUJRVThzUTBGQlF5eGpRVUZqTEVOQlFVTXNUMEZCVHl4RFFVRkRMR05CUVdNc1EwRkJReXhOUVVGTkxFZEJRVWNzUTBGQlF5eERRVUZETEVOQlFVTXNSMEZCUnp0blFrRkRMMFFzUTBGQlF5eERRVUZETEU5QlFVOHNRMEZCUXl4WFFVRlhMRU5CUVVNN1UwRkRla0k3WVVGQlRUdFpRVU5NTEhWRVFVRjFSRHRaUVVOMlJDeDNSa0ZCZDBZN1dVRkRlRVlzUjBGQlJ5eEhRVUZITEU5QlFVOHNRMEZCUXl4WFFVRlhMRU5CUVVNN1UwRkRNMEk3VVVGRFJDeFBRVUZQTEVkQlFVY3NRMEZCUXp0SlFVTmlMRU5CUVVNN1NVRkZUeXhMUVVGTExFTkJRVU1zZFVKQlFYVkNMRU5CUTI1RExFOUJRU3RETEVWQlF5OURMRTlCUVU4c1JVRkRVQ3haUVVGdlFqdFJRVVZ3UWpzN096czdPMVZCVFVVN1VVRkZSaXcwUWtGQk5FSTdVVUZETlVJc2FVUkJRV2xFTzFGQlJXcEVPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3pzN096czdPenM3T3p0VlFUSkVSVHRSUVVWR0xFMUJRVTBzWTBGQll5eEhRVUZITEU5QlFVOHNRMEZCUXl4VlFVRlZMRU5CUVVNN1VVRkRNVU1zVFVGQlRTeHJRa0ZCYTBJc1IwRkJSeXhQUVVGUExFTkJRVU1zVlVGQlZTeERRVUZETzFGQlJUbERMRTFCUVUwc1IwRkJSeXhIUVVOUUxFOUJRVThzUTBGQlF5eExRVUZMTEVkQlFVY3NRMEZCUXl4RFFVRkRPMWxCUTJoQ0xFTkJRVU1zUTBGQlF5eE5RVUZOTEU5QlFVOHNRMEZCUXl4SlFVRkpMRU5CUVVNc1IwRkJSeXhEUVVGRExFOUJRVThzUTBGQlF5eExRVUZMTEVOQlFVTTdXVUZEZGtNc1EwRkJReXhEUVVGRExFVkJRVVVzVVVGQlVTeEZRVUZGTEZOQlFWTXNSVUZCUlN4VFFVRlRMRVZCUVVVc1UwRkJVeXhGUVVGRkxFTkJRVU03VVVGRGNFUXNUVUZCVFN4WlFVRlpMRWRCUVdsQ08xbEJRMnBETEZOQlFWTXNSVUZCUlN4VFFVRlRMRU5CUVVNc1IwRkJSeXhEUVVGRExGTkJRVk1zUTBGQlF6dFpRVU51UXl4VlFVRlZMRVZCUVVVc1QwRkJUenRaUVVOdVFpeGxRVUZsTEVWQlFVVXNVMEZCVXl4RFFVRkRMRTlCUVU4c1EwRkJReXhIUVVGSExFTkJRVU03V1VGRGRrTXNZMEZCWXl4RlFVRkZMRTlCUVU4c1EwRkJReXhUUVVGVE8xbEJRMnBETEdWQlFXVXNSVUZCUlN4VFFVRlRMRU5CUVVNc1QwRkJUeXhEUVVGRExGZEJRVmNzUTBGQlF6dFpRVU12UXl4alFVRmpMRVZCUVVVc1NVRkJTVHRaUVVOd1FpeHpRa0ZCYzBJc1JVRkJSU3h2UWtGQmIwSTdXVUZETlVNc1lVRkJZU3hGUVVGRkxGbEJRVms3V1VGRE0wSXNVMEZCVXl4RlFVRkZMRWRCUVVjc1EwRkJReXhSUVVGUk8xbEJRM1pDTEUxQlFVMHNSVUZCUlN4UFFVRlBMRU5CUVVNc1MwRkJTenRaUVVOeVFpeFJRVUZSTEVWQlFVVXNUMEZCVHl4RFFVRkRMRTlCUVU4N1dVRkRla0lzWlVGQlpTeEZRVUZGTEdOQlFXTTdXVUZETDBJc2IwSkJRVzlDTEVWQlFVVXNXVUZCV1N4RFFVRkRMR3RDUVVGclFpeERRVUZETzFsQlEzUkVMRTlCUVU4c1JVRkJSU3hKUVVGSkxFTkJRVU1zWTBGQll5eERRVUZETEU5QlFVOHNRMEZCUXl4bFFVRmxMRU5CUVVNc1EwRkJReXhQUVVGUE8xbEJRemRFTEZWQlFWVXNSVUZCUlN4SlFVRkpMRWxCUVVrc1EwRkJReXhQUVVGUExFTkJRVU1zVTBGQlV5eERRVUZETEVOQlFVTXNWMEZCVnl4RlFVRkZPMU5CUTNSRUxFTkJRVU03VVVGRlJpeEpRVUZKTEVOQlFVTXNXVUZCV1N4RFFVRkRMRlZCUVZVc1EwRkJReXhuUWtGQlowSXNSVUZCUlN4WlFVRlpMRU5CUVVNc1EwRkJRenRKUVVNdlJDeERRVUZETzBsQlJVUTdPMDlCUlVjN1NVRkZTeXhMUVVGTExFTkJRVU1zYlVKQlFXMUNMRU5CUXk5Q0xFOUJRVGhETEVWQlF6bERMRTFCUVc5Q08xRkJSWEJDTEUxQlFVMHNaVUZCWlN4SFFVRkhMRWxCUVVrc1EwRkJReXhyUWtGQmEwSXNRMEZCUXl4UFFVRlBMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU03VVVGRGJrVXNTVUZCU1R0WlFVTkdMRTFCUVUwc2IwSkJRVzlDTEVkQlFVY3NaVUZCWlN4RFFVRkRMRzlDUVVGdlFpeERRVUZETzFsQlEyeEZMRTFCUVUwc1VVRkJVU3hIUVVGSExFMUJRVTBzYjBKQlFXOUNMRU5CUVVNc1pVRkJaU3hGUVVGRkxFTkJRVU03V1VGRE9VUXNUVUZCVFN4WFFVRlhMRWRCUVVjc1RVRkJUU3h2UWtGQmIwSXNRMEZCUXl4alFVRmpMRVZCUVVVc1EwRkJRenRaUVVOb1JTeEpRVUZKTEVOQlFVTXNXVUZCV1N4RFFVRkRMRmRCUVZjc1EwRkJReXhSUVVGUkxFVkJRVVVzV1VGQldTeERRVUZETEZkQlFWY3NRMEZCUXl4RFFVRkRMRU5CUVVNN1dVRkRia1VzVFVGQlRTeERRVUZETEZsQlFWa3NSMEZCUnl4WFFVRlhMRU5CUVVNN1dVRkRiRU1zU1VGQlNTeERRVUZETEZsQlFWa3NRMEZCUXl4VlFVRlZMRU5CUVVNc1owSkJRV2RDTEVWQlFVVXNUVUZCVFN4RFFVRkRMRU5CUVVNN1UwRkRlRVE3VVVGQlF5eFBRVUZQTEVkQlFVY3NSVUZCUlR0WlFVTmFPenM3T3pzN08yTkJUMFU3V1VGRFJpeEpRVUZKTEVOQlFVTXNXVUZCV1N4RFFVRkRMRkZCUVZFc1EwRkRlRUlzYlVOQlFXMURPMmRDUVVOcVF5eHpSRUZCYzBRN1owSkJRM1JFTEVkQlFVY3NRMEZCUXl4SlFVRkpPMmRDUVVOU0xFZEJRVWNzUTBGQlF5eFBRVUZQTzJkQ1FVTllMRWxCUVVrN1owSkJRMG9zUjBGQlJ5eERRVUZETEV0QlFVc3NRMEZEV2l4RFFVRkRPMWxCUTBZc1RVRkJUU3hEUVVGRExGbEJRVmtzUjBGQlJ5eFRRVUZUTEVOQlFVTTdXVUZEYUVNc1NVRkJTU3hEUVVGRExGbEJRVmtzUTBGQlF5eFZRVUZWTEVOQlFVTXNaMEpCUVdkQ0xFVkJRVVVzVFVGQlRTeERRVUZETEVOQlFVTTdVMEZEZUVRN1NVRkRTQ3hEUVVGRE8wbEJSVVFzTkVKQlFUUkNPMGxCUTNCQ0xFdEJRVXNzUTBGQlF5eHJRa0ZCYTBJc1EwRkRPVUlzVDBGQk1FTXNSVUZETVVNc1QwRkJUeXhGUVVOUUxGbEJRVmtzUlVGRFdpeFhRVUZYTzFGQlJWZzdPenM3T3pzN1ZVRlBSVHRSUVVWR0xFMUJRVTBzUjBGQlJ5eEhRVU5RTEU5QlFVOHNRMEZCUXl4TFFVRkxMRWRCUVVjc1EwRkJReXhEUVVGRE8xbEJRMmhDTEVOQlFVTXNRMEZCUXl4TlFVRk5MRTlCUVU4c1EwRkJReXhKUVVGSkxFTkJRVU1zUjBGQlJ5eERRVUZETEU5QlFVOHNRMEZCUXl4TFFVRkxMRU5CUVVNN1dVRkRka01zUTBGQlF5eERRVUZETEVWQlFVVXNVVUZCVVN4RlFVRkZMRk5CUVZNc1JVRkJSU3hUUVVGVExFVkJRVVVzVTBGQlV5eEZRVUZGTEVOQlFVTTdVVUZGY0VRc1RVRkJUU3hOUVVGTkxFZEJRVWNzUlVGQmEwSXNRMEZCUXp0UlFVVnNReXhOUVVGTkxFTkJRVU1zVTBGQlV5eEhRVUZITEZOQlFWTXNRMEZCUXl4SFFVRkhMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU03VVVGRE5VTXNUVUZCVFN4RFFVRkRMRlZCUVZVc1IwRkJSeXhQUVVGUExFTkJRVU03VVVGRE5VSXNUVUZCVFN4RFFVRkRMSE5DUVVGelFpeEhRVUZITEc5Q1FVRnZRaXhEUVVGRE8xRkJRM0pFTEUxQlFVMHNRMEZCUXl4aFFVRmhMRWRCUVVjc1dVRkJXU3hEUVVGRE8xRkJRM0JETEUxQlFVMHNRMEZCUXl4VFFVRlRMRWRCUVVjc1IwRkJSeXhEUVVGRExGRkJRVkVzUTBGQlF6dFJRVU5vUXl4TlFVRk5MRU5CUVVNc1RVRkJUU3hIUVVGSExFOUJRVThzUTBGQlF5eExRVUZMTEVOQlFVTTdVVUZET1VJc1RVRkJUU3hEUVVGRExGRkJRVkVzUjBGQlJ5eFBRVUZQTEVOQlFVTXNUMEZCVHl4RFFVRkRPMUZCUld4RExHMUdRVUZ0Ump0UlFVTnVSaXhOUVVGTkxFTkJRVU1zVlVGQlZTeEhRVUZITEUxQlFVMHNRMEZCUXl4UFFVRlBMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU03VVVGRk9VTXNUVUZCVFN4UlFVRlJMRWRCUVVjc1QwRkJUeXhEUVVGRExGTkJRVk1zUTBGQlF6dFJRVU51UXl4TlFVRk5MRU5CUVVNc1UwRkJVeXhIUVVGSExGTkJRVk1zUTBGQlF5eFJRVUZSTEVOQlFVTXNRMEZCUXp0UlFVVjJReXhOUVVGTkxFZEJRVWNzUjBGQlJ5eFBRVUZQTEVOQlFVTXNSMEZCUnl4RFFVRkRPMUZCUTNoQ0xFMUJRVTBzUTBGQlF5eEhRVUZITEVkQlFVY3NVMEZCVXl4RFFVRkRMRWRCUVVjc1EwRkJReXhEUVVGRE8xRkJSVFZDTEUxQlFVMHNZVUZCWVN4SFFVRkhMRTlCUVU4c1EwRkJReXhOUVVGTkxFTkJRVU03VVVGRGNrTXNUVUZCVFN4RFFVRkRMRTFCUVUwc1IwRkJSeXhaUVVGWkxFTkJRVU1zWVVGQllTeERRVUZETEVOQlFVTTdVVUZGTlVNc01FUkJRVEJFTzFGQlF6RkVMRFpGUVVFMlJUdFJRVU0zUlN3eVJVRkJNa1U3VVVGRE0wVXNSVUZCUlR0UlFVTkdMSEZDUVVGeFFqdFJRVU55UWl3d1FrRkJNRUk3VVVGRE1VSXNjME5CUVhORE8xRkJRM1JETEVsQlFVazdVVUZEU2l3MFEwRkJORU03VVVGRk5VTXNUVUZCVFN4alFVRmpMRWRCUVVjc1QwRkJUeXhEUVVGRExGVkJRVlVzUTBGQlF6dFJRVU14UXl4TlFVRk5MRU5CUVVNc1pVRkJaU3hIUVVGSExHTkJRV01zUTBGQlF6dFJRVVY0UXl4TlFVRk5MR3RDUVVGclFpeEhRVUZITEU5QlFVOHNRMEZCUXl4VlFVRlZMRU5CUVVNN1VVRkRPVU1zVFVGQlRTeERRVUZETEc5Q1FVRnZRaXhIUVVGSExGbEJRVmtzUTBGQlF5eHJRa0ZCYTBJc1EwRkJReXhEUVVGRE8xRkJSUzlFTEUxQlFVMHNXVUZCV1N4SFFVRkhMRWxCUVVrc1NVRkJTU3hEUVVGRExFOUJRVThzUTBGQlF5eFRRVUZUTEVOQlFVTXNRMEZCUXp0UlFVTnFSQ3hOUVVGTkxFTkJRVU1zVlVGQlZTeEhRVUZITEZsQlFWa3NRMEZCUXl4WFFVRlhMRVZCUVVVc1EwRkJRenRSUVVVdlF5eE5RVUZOTEdGQlFXRXNSMEZCUnl4SlFVRkpMRU5CUVVNc1kwRkJZeXhEUVVGRExFOUJRVThzUTBGQlF5eGxRVUZsTEVOQlFVTXNRMEZCUXp0UlFVTnVSU3hOUVVGTkxFTkJRVU1zVDBGQlR5eEhRVUZITEdGQlFXRXNRMEZCUXl4UFFVRlBMRU5CUVVNN1VVRkRka01zVFVGQlRTeERRVUZETEZGQlFWRXNSMEZCUnl4aFFVRmhMRU5CUVVNc1VVRkJVU3hEUVVGRE8xRkJSWHBETEVsQlFVa3NTVUZCU1N4RFFVRkRMR2xDUVVGcFFpeERRVUZETEZkQlFWY3NSVUZCUlN4UFFVRlBMRU5CUVVNc1NVRkJTU3hEUVVGRExFVkJRVVU3V1VGRGNrUXNTVUZCU1N4RFFVRkRMRzFDUVVGdFFpeERRVUZETEU5QlFVOHNSVUZCUlN4TlFVRk5MRU5CUVVNc1EwRkJRenRUUVVNelF6dGhRVUZOTzFsQlEwd3NTVUZCU1N4RFFVRkRMRmxCUVZrc1EwRkJReXhWUVVGVkxFTkJRVU1zWjBKQlFXZENMRVZCUVVVc1RVRkJUU3hEUVVGRExFTkJRVU03VTBGRGVFUTdTVUZEU0N4RFFVRkRPMGxCUlU4c1kwRkJZeXhEUVVGRExFOUJRVzlDTzFGQlEzcERMRTFCUVUwc1lVRkJZU3hIUVVGSExFVkJRVVVzUTBGQlF6dFJRVU42UWl4SlFVRkpMRkZCUVZFc1IwRkJSeXhGUVVGRkxFTkJRVU03VVVGRGJFSXNTVUZCU1N4UFFVRlBMRVZCUVVVN1dVRkRXQ3hQUVVGUExFTkJRVU1zUjBGQlJ5eERRVUZETEVOQlFVTXNZMEZCWXl4RlFVRkZMRVZCUVVVN1owSkJRemRDTEUxQlFVMHNSVUZCUlN4SlFVRkpMRVZCUVVVc1MwRkJTeXhGUVVGRkxFZEJRVWNzWTBGQll5eERRVUZETzJkQ1FVTjJReXhOUVVGTkxGZEJRVmNzUjBGQlJ5eEZRVUZGTEVOQlFVTTdaMEpCUTNaQ0xGZEJRVmNzUTBGQlF5eEpRVUZKTEVOQlFVTXNXVUZCV1N4RFFVRkRMRWxCUVVrc1EwRkJReXhEUVVGRExFTkJRVU03WjBKQlEzSkRMRmRCUVZjc1EwRkJReXhKUVVGSkxFTkJRVU1zV1VGQldTeERRVUZETEV0QlFVc3NRMEZCUXl4RFFVRkRMRU5CUVVNN1owSkJRM1JETEdGQlFXRXNRMEZCUXl4SlFVRkpMRU5CUVVNc1YwRkJWeXhEUVVGRExFTkJRVU03WjBKQlEyaERMRWxCUVVrc1NVRkJTU3hEUVVGRExGZEJRVmNzUlVGQlJTeExRVUZMTEZWQlFWVXNSVUZCUlR0dlFrRkRja01zVVVGQlVTeEhRVUZITEV0QlFVc3NRMEZCUXp0cFFrRkRiRUk3V1VGRFNDeERRVUZETEVOQlFVTXNRMEZCUXp0VFFVTktPMUZCUTBRc1QwRkJUenRaUVVOTUxFOUJRVThzUlVGQlJTeEpRVUZKTEVOQlFVTXNVMEZCVXl4RFFVRkRMR0ZCUVdFc1EwRkJRenRaUVVOMFF5eFJRVUZSTEVWQlFVVXNXVUZCV1N4RFFVRkRMRkZCUVZFc1EwRkJRenRUUVVOcVF5eERRVUZETzBsQlEwb3NRMEZCUXp0RFFVTkdJbjA9IiwiaW1wb3J0IHsgaW5jcmVtZW50ZWRFdmVudE9yZGluYWwgfSBmcm9tIFwiLi4vbGliL2V4dGVuc2lvbi1zZXNzaW9uLWV2ZW50LW9yZGluYWxcIjtcbmltcG9ydCB7IGV4dGVuc2lvblNlc3Npb25VdWlkIH0gZnJvbSBcIi4uL2xpYi9leHRlbnNpb24tc2Vzc2lvbi11dWlkXCI7XG5pbXBvcnQgeyBib29sVG9JbnQsIGVzY2FwZVN0cmluZywgZXNjYXBlVXJsIH0gZnJvbSBcIi4uL2xpYi9zdHJpbmctdXRpbHNcIjtcbmV4cG9ydCBjbGFzcyBKYXZhc2NyaXB0SW5zdHJ1bWVudCB7XG4gICAgLyoqXG4gICAgICogQ29udmVydHMgcmVjZWl2ZWQgY2FsbCBhbmQgdmFsdWVzIGRhdGEgZnJvbSB0aGUgSlMgSW5zdHJ1bWVudGF0aW9uXG4gICAgICogaW50byB0aGUgZm9ybWF0IHRoYXQgdGhlIHNjaGVtYSBleHBlY3RzLlxuICAgICAqXG4gICAgICogQHBhcmFtIGRhdGFcbiAgICAgKiBAcGFyYW0gc2VuZGVyXG4gICAgICovXG4gICAgc3RhdGljIHByb2Nlc3NDYWxsc0FuZFZhbHVlcyhkYXRhLCBzZW5kZXIpIHtcbiAgICAgICAgY29uc3QgdXBkYXRlID0ge307XG4gICAgICAgIHVwZGF0ZS5leHRlbnNpb25fc2Vzc2lvbl91dWlkID0gZXh0ZW5zaW9uU2Vzc2lvblV1aWQ7XG4gICAgICAgIHVwZGF0ZS5ldmVudF9vcmRpbmFsID0gaW5jcmVtZW50ZWRFdmVudE9yZGluYWwoKTtcbiAgICAgICAgdXBkYXRlLnBhZ2Vfc2NvcGVkX2V2ZW50X29yZGluYWwgPSBkYXRhLm9yZGluYWw7XG4gICAgICAgIHVwZGF0ZS53aW5kb3dfaWQgPSBzZW5kZXIudGFiLndpbmRvd0lkO1xuICAgICAgICB1cGRhdGUudGFiX2lkID0gc2VuZGVyLnRhYi5pZDtcbiAgICAgICAgdXBkYXRlLmZyYW1lX2lkID0gc2VuZGVyLmZyYW1lSWQ7XG4gICAgICAgIHVwZGF0ZS5zY3JpcHRfdXJsID0gZXNjYXBlVXJsKGRhdGEuc2NyaXB0VXJsKTtcbiAgICAgICAgdXBkYXRlLnNjcmlwdF9saW5lID0gZXNjYXBlU3RyaW5nKGRhdGEuc2NyaXB0TGluZSk7XG4gICAgICAgIHVwZGF0ZS5zY3JpcHRfY29sID0gZXNjYXBlU3RyaW5nKGRhdGEuc2NyaXB0Q29sKTtcbiAgICAgICAgdXBkYXRlLmZ1bmNfbmFtZSA9IGVzY2FwZVN0cmluZyhkYXRhLmZ1bmNOYW1lKTtcbiAgICAgICAgdXBkYXRlLnNjcmlwdF9sb2NfZXZhbCA9IGVzY2FwZVN0cmluZyhkYXRhLnNjcmlwdExvY0V2YWwpO1xuICAgICAgICB1cGRhdGUuY2FsbF9zdGFjayA9IGVzY2FwZVN0cmluZyhkYXRhLmNhbGxTdGFjayk7XG4gICAgICAgIHVwZGF0ZS5zeW1ib2wgPSBlc2NhcGVTdHJpbmcoZGF0YS5zeW1ib2wpO1xuICAgICAgICB1cGRhdGUub3BlcmF0aW9uID0gZXNjYXBlU3RyaW5nKGRhdGEub3BlcmF0aW9uKTtcbiAgICAgICAgdXBkYXRlLnZhbHVlID0gZXNjYXBlU3RyaW5nKGRhdGEudmFsdWUpO1xuICAgICAgICB1cGRhdGUuYXR0cmlidXRlcyA9IGVzY2FwZVN0cmluZyhkYXRhLmF0dHJpYnV0ZXMpO1xuICAgICAgICB1cGRhdGUudGltZV9zdGFtcCA9IGRhdGEudGltZVN0YW1wO1xuICAgICAgICB1cGRhdGUuaW5jb2duaXRvID0gYm9vbFRvSW50KHNlbmRlci50YWIuaW5jb2duaXRvKTtcbiAgICAgICAgLy8gZG9jdW1lbnRfdXJsIGlzIHRoZSBjdXJyZW50IGZyYW1lJ3MgZG9jdW1lbnQgaHJlZlxuICAgICAgICAvLyB0b3BfbGV2ZWxfdXJsIGlzIHRoZSB0b3AtbGV2ZWwgZnJhbWUncyBkb2N1bWVudCBocmVmXG4gICAgICAgIHVwZGF0ZS5kb2N1bWVudF91cmwgPSBlc2NhcGVVcmwoc2VuZGVyLnVybCk7XG4gICAgICAgIHVwZGF0ZS50b3BfbGV2ZWxfdXJsID0gZXNjYXBlVXJsKHNlbmRlci50YWIudXJsKTtcbiAgICAgICAgaWYgKGRhdGEub3BlcmF0aW9uID09PSBcImNhbGxcIiAmJiBkYXRhLmFyZ3MubGVuZ3RoID4gMCkge1xuICAgICAgICAgICAgdXBkYXRlLmFyZ3VtZW50cyA9IGVzY2FwZVN0cmluZyhKU09OLnN0cmluZ2lmeShkYXRhLmFyZ3MpKTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gdXBkYXRlO1xuICAgIH1cbiAgICBkYXRhUmVjZWl2ZXI7XG4gICAgb25NZXNzYWdlTGlzdGVuZXI7XG4gICAgY29uZmlndXJlZCA9IGZhbHNlO1xuICAgIHBlbmRpbmdSZWNvcmRzID0gW107XG4gICAgY3Jhd2xJRDtcbiAgICBjb25zdHJ1Y3RvcihkYXRhUmVjZWl2ZXIpIHtcbiAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIgPSBkYXRhUmVjZWl2ZXI7XG4gICAgfVxuICAgIC8qKlxuICAgICAqIFN0YXJ0IGxpc3RlbmluZyBmb3IgbWVzc2FnZXMgZnJvbSBwYWdlL2NvbnRlbnQvYmFja2dyb3VuZCBzY3JpcHRzIGluamVjdGVkIHRvIGluc3RydW1lbnQgSmF2YVNjcmlwdCBBUElzXG4gICAgICovXG4gICAgbGlzdGVuKCkge1xuICAgICAgICB0aGlzLm9uTWVzc2FnZUxpc3RlbmVyID0gKG1lc3NhZ2UsIHNlbmRlcikgPT4ge1xuICAgICAgICAgICAgaWYgKG1lc3NhZ2UubmFtZXNwYWNlICYmXG4gICAgICAgICAgICAgICAgbWVzc2FnZS5uYW1lc3BhY2UgPT09IFwiamF2YXNjcmlwdC1pbnN0cnVtZW50YXRpb25cIikge1xuICAgICAgICAgICAgICAgIHRoaXMuaGFuZGxlSnNJbnN0cnVtZW50YXRpb25NZXNzYWdlKG1lc3NhZ2UsIHNlbmRlcik7XG4gICAgICAgICAgICB9XG4gICAgICAgIH07XG4gICAgICAgIGJyb3dzZXIucnVudGltZS5vbk1lc3NhZ2UuYWRkTGlzdGVuZXIodGhpcy5vbk1lc3NhZ2VMaXN0ZW5lcik7XG4gICAgfVxuICAgIC8qKlxuICAgICAqIEVpdGhlciBzZW5kcyB0aGUgbG9nIGRhdGEgdG8gdGhlIGRhdGFSZWNlaXZlciBvciBzdG9yZSBpdCBpbiBtZW1vcnlcbiAgICAgKiBhcyBhIHBlbmRpbmcgcmVjb3JkIGlmIHRoZSBKUyBpbnN0cnVtZW50YXRpb24gaXMgbm90IHlldCBjb25maWd1cmVkXG4gICAgICpcbiAgICAgKiBAcGFyYW0gbWVzc2FnZVxuICAgICAqIEBwYXJhbSBzZW5kZXJcbiAgICAgKi9cbiAgICBoYW5kbGVKc0luc3RydW1lbnRhdGlvbk1lc3NhZ2UobWVzc2FnZSwgc2VuZGVyKSB7XG4gICAgICAgIHN3aXRjaCAobWVzc2FnZS50eXBlKSB7XG4gICAgICAgICAgICBjYXNlIFwibG9nQ2FsbFwiOlxuICAgICAgICAgICAgY2FzZSBcImxvZ1ZhbHVlXCI6XG4gICAgICAgICAgICAgICAgY29uc3QgdXBkYXRlID0gSmF2YXNjcmlwdEluc3RydW1lbnQucHJvY2Vzc0NhbGxzQW5kVmFsdWVzKG1lc3NhZ2UuZGF0YSwgc2VuZGVyKTtcbiAgICAgICAgICAgICAgICBpZiAodGhpcy5jb25maWd1cmVkKSB7XG4gICAgICAgICAgICAgICAgICAgIHVwZGF0ZS5icm93c2VyX2lkID0gdGhpcy5jcmF3bElEO1xuICAgICAgICAgICAgICAgICAgICB0aGlzLmRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwiamF2YXNjcmlwdFwiLCB1cGRhdGUpO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgdGhpcy5wZW5kaW5nUmVjb3Jkcy5wdXNoKHVwZGF0ZSk7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICB9XG4gICAgfVxuICAgIC8qKlxuICAgICAqIFN0YXJ0cyBsaXN0ZW5pbmcgaWYgaGF2ZW4ndCBkb25lIHNvIGFscmVhZHksIHNldHMgdGhlIGNyYXdsIElELFxuICAgICAqIG1hcmtzIHRoZSBKUyBpbnN0cnVtZW50YXRpb24gYXMgY29uZmlndXJlZCBhbmQgc2VuZHMgYW55IHBlbmRpbmdcbiAgICAgKiByZWNvcmRzIHRoYXQgaGF2ZSBiZWVuIHJlY2VpdmVkIHVwIHVudGlsIHRoaXMgcG9pbnQuXG4gICAgICpcbiAgICAgKiBAcGFyYW0gY3Jhd2xJRFxuICAgICAqL1xuICAgIHJ1bihjcmF3bElEKSB7XG4gICAgICAgIGlmICghdGhpcy5vbk1lc3NhZ2VMaXN0ZW5lcikge1xuICAgICAgICAgICAgdGhpcy5saXN0ZW4oKTtcbiAgICAgICAgfVxuICAgICAgICB0aGlzLmNyYXdsSUQgPSBjcmF3bElEO1xuICAgICAgICB0aGlzLmNvbmZpZ3VyZWQgPSB0cnVlO1xuICAgICAgICB0aGlzLnBlbmRpbmdSZWNvcmRzLm1hcCgodXBkYXRlKSA9PiB7XG4gICAgICAgICAgICB1cGRhdGUuYnJvd3Nlcl9pZCA9IHRoaXMuY3Jhd2xJRDtcbiAgICAgICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyLnNhdmVSZWNvcmQoXCJqYXZhc2NyaXB0XCIsIHVwZGF0ZSk7XG4gICAgICAgIH0pO1xuICAgIH1cbiAgICBhc3luYyByZWdpc3RlckNvbnRlbnRTY3JpcHQodGVzdGluZywganNJbnN0cnVtZW50YXRpb25TZXR0aW5ncykge1xuICAgICAgICBjb25zdCBjb250ZW50U2NyaXB0Q29uZmlnID0ge1xuICAgICAgICAgICAgdGVzdGluZyxcbiAgICAgICAgICAgIGpzSW5zdHJ1bWVudGF0aW9uU2V0dGluZ3MsXG4gICAgICAgIH07XG4gICAgICAgIGlmIChjb250ZW50U2NyaXB0Q29uZmlnKSB7XG4gICAgICAgICAgICAvLyBUT0RPOiBBdm9pZCB1c2luZyB3aW5kb3cgdG8gcGFzcyB0aGUgY29udGVudCBzY3JpcHQgY29uZmlnXG4gICAgICAgICAgICBhd2FpdCBicm93c2VyLmNvbnRlbnRTY3JpcHRzLnJlZ2lzdGVyKHtcbiAgICAgICAgICAgICAgICBqczogW1xuICAgICAgICAgICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgICAgICAgICBjb2RlOiBgd2luZG93Lm9wZW5XcG1Db250ZW50U2NyaXB0Q29uZmlnID0gJHtKU09OLnN0cmluZ2lmeShjb250ZW50U2NyaXB0Q29uZmlnKX07YCxcbiAgICAgICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBdLFxuICAgICAgICAgICAgICAgIG1hdGNoZXM6IFtcIjxhbGxfdXJscz5cIl0sXG4gICAgICAgICAgICAgICAgYWxsRnJhbWVzOiB0cnVlLFxuICAgICAgICAgICAgICAgIHJ1bkF0OiBcImRvY3VtZW50X3N0YXJ0XCIsXG4gICAgICAgICAgICAgICAgbWF0Y2hBYm91dEJsYW5rOiB0cnVlLFxuICAgICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGJyb3dzZXIuY29udGVudFNjcmlwdHMucmVnaXN0ZXIoe1xuICAgICAgICAgICAganM6IFt7IGZpbGU6IFwiL2NvbnRlbnQuanNcIiB9XSxcbiAgICAgICAgICAgIG1hdGNoZXM6IFtcIjxhbGxfdXJscz5cIl0sXG4gICAgICAgICAgICBhbGxGcmFtZXM6IHRydWUsXG4gICAgICAgICAgICBydW5BdDogXCJkb2N1bWVudF9zdGFydFwiLFxuICAgICAgICAgICAgbWF0Y2hBYm91dEJsYW5rOiB0cnVlLFxuICAgICAgICB9KTtcbiAgICB9XG4gICAgY2xlYW51cCgpIHtcbiAgICAgICAgdGhpcy5wZW5kaW5nUmVjb3JkcyA9IFtdO1xuICAgICAgICBpZiAodGhpcy5vbk1lc3NhZ2VMaXN0ZW5lcikge1xuICAgICAgICAgICAgYnJvd3Nlci5ydW50aW1lLm9uTWVzc2FnZS5yZW1vdmVMaXN0ZW5lcih0aGlzLm9uTWVzc2FnZUxpc3RlbmVyKTtcbiAgICAgICAgfVxuICAgIH1cbn1cbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWFtRjJZWE5qY21sd2RDMXBibk4wY25WdFpXNTBMbXB6SWl3aWMyOTFjbU5sVW05dmRDSTZJaUlzSW5OdmRYSmpaWE1pT2xzaUxpNHZMaTR2TGk0dmMzSmpMMkpoWTJ0bmNtOTFibVF2YW1GMllYTmpjbWx3ZEMxcGJuTjBjblZ0Wlc1MExuUnpJbDBzSW01aGJXVnpJanBiWFN3aWJXRndjR2x1WjNNaU9pSkJRVU5CTEU5QlFVOHNSVUZCUlN4MVFrRkJkVUlzUlVGQlJTeE5RVUZOTEhkRFFVRjNReXhEUVVGRE8wRkJRMnBHTEU5QlFVOHNSVUZCUlN4dlFrRkJiMElzUlVGQlJTeE5RVUZOTEN0Q1FVRXJRaXhEUVVGRE8wRkJRM0pGTEU5QlFVOHNSVUZCUlN4VFFVRlRMRVZCUVVVc1dVRkJXU3hGUVVGRkxGTkJRVk1zUlVGQlJTeE5RVUZOTEhGQ1FVRnhRaXhEUVVGRE8wRkJTWHBGTEUxQlFVMHNUMEZCVHl4dlFrRkJiMEk3U1VGREwwSTdPenM3T3p0UFFVMUhPMGxCUTBzc1RVRkJUU3hEUVVGRExIRkNRVUZ4UWl4RFFVRkRMRWxCUVVrc1JVRkJSU3hOUVVGeFFqdFJRVU01UkN4TlFVRk5MRTFCUVUwc1IwRkJSeXhGUVVGNVFpeERRVUZETzFGQlEzcERMRTFCUVUwc1EwRkJReXh6UWtGQmMwSXNSMEZCUnl4dlFrRkJiMElzUTBGQlF6dFJRVU55UkN4TlFVRk5MRU5CUVVNc1lVRkJZU3hIUVVGSExIVkNRVUYxUWl4RlFVRkZMRU5CUVVNN1VVRkRha1FzVFVGQlRTeERRVUZETEhsQ1FVRjVRaXhIUVVGSExFbEJRVWtzUTBGQlF5eFBRVUZQTEVOQlFVTTdVVUZEYUVRc1RVRkJUU3hEUVVGRExGTkJRVk1zUjBGQlJ5eE5RVUZOTEVOQlFVTXNSMEZCUnl4RFFVRkRMRkZCUVZFc1EwRkJRenRSUVVOMlF5eE5RVUZOTEVOQlFVTXNUVUZCVFN4SFFVRkhMRTFCUVUwc1EwRkJReXhIUVVGSExFTkJRVU1zUlVGQlJTeERRVUZETzFGQlF6bENMRTFCUVUwc1EwRkJReXhSUVVGUkxFZEJRVWNzVFVGQlRTeERRVUZETEU5QlFVOHNRMEZCUXp0UlFVTnFReXhOUVVGTkxFTkJRVU1zVlVGQlZTeEhRVUZITEZOQlFWTXNRMEZCUXl4SlFVRkpMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU03VVVGRE9VTXNUVUZCVFN4RFFVRkRMRmRCUVZjc1IwRkJSeXhaUVVGWkxFTkJRVU1zU1VGQlNTeERRVUZETEZWQlFWVXNRMEZCUXl4RFFVRkRPMUZCUTI1RUxFMUJRVTBzUTBGQlF5eFZRVUZWTEVkQlFVY3NXVUZCV1N4RFFVRkRMRWxCUVVrc1EwRkJReXhUUVVGVExFTkJRVU1zUTBGQlF6dFJRVU5xUkN4TlFVRk5MRU5CUVVNc1UwRkJVeXhIUVVGSExGbEJRVmtzUTBGQlF5eEpRVUZKTEVOQlFVTXNVVUZCVVN4RFFVRkRMRU5CUVVNN1VVRkRMME1zVFVGQlRTeERRVUZETEdWQlFXVXNSMEZCUnl4WlFVRlpMRU5CUVVNc1NVRkJTU3hEUVVGRExHRkJRV0VzUTBGQlF5eERRVUZETzFGQlF6RkVMRTFCUVUwc1EwRkJReXhWUVVGVkxFZEJRVWNzV1VGQldTeERRVUZETEVsQlFVa3NRMEZCUXl4VFFVRlRMRU5CUVVNc1EwRkJRenRSUVVOcVJDeE5RVUZOTEVOQlFVTXNUVUZCVFN4SFFVRkhMRmxCUVZrc1EwRkJReXhKUVVGSkxFTkJRVU1zVFVGQlRTeERRVUZETEVOQlFVTTdVVUZETVVNc1RVRkJUU3hEUVVGRExGTkJRVk1zUjBGQlJ5eFpRVUZaTEVOQlFVTXNTVUZCU1N4RFFVRkRMRk5CUVZNc1EwRkJReXhEUVVGRE8xRkJRMmhFTEUxQlFVMHNRMEZCUXl4TFFVRkxMRWRCUVVjc1dVRkJXU3hEUVVGRExFbEJRVWtzUTBGQlF5eExRVUZMTEVOQlFVTXNRMEZCUXp0UlFVTjRReXhOUVVGTkxFTkJRVU1zVlVGQlZTeEhRVUZITEZsQlFWa3NRMEZCUXl4SlFVRkpMRU5CUVVNc1ZVRkJWU3hEUVVGRExFTkJRVU03VVVGRGJFUXNUVUZCVFN4RFFVRkRMRlZCUVZVc1IwRkJSeXhKUVVGSkxFTkJRVU1zVTBGQlV5eERRVUZETzFGQlEyNURMRTFCUVUwc1EwRkJReXhUUVVGVExFZEJRVWNzVTBGQlV5eERRVUZETEUxQlFVMHNRMEZCUXl4SFFVRkhMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU03VVVGRmJrUXNiMFJCUVc5RU8xRkJRM0JFTEhWRVFVRjFSRHRSUVVOMlJDeE5RVUZOTEVOQlFVTXNXVUZCV1N4SFFVRkhMRk5CUVZNc1EwRkJReXhOUVVGTkxFTkJRVU1zUjBGQlJ5eERRVUZETEVOQlFVTTdVVUZETlVNc1RVRkJUU3hEUVVGRExHRkJRV0VzUjBGQlJ5eFRRVUZUTEVOQlFVTXNUVUZCVFN4RFFVRkRMRWRCUVVjc1EwRkJReXhIUVVGSExFTkJRVU1zUTBGQlF6dFJRVVZxUkN4SlFVRkpMRWxCUVVrc1EwRkJReXhUUVVGVExFdEJRVXNzVFVGQlRTeEpRVUZKTEVsQlFVa3NRMEZCUXl4SlFVRkpMRU5CUVVNc1RVRkJUU3hIUVVGSExFTkJRVU1zUlVGQlJUdFpRVU55UkN4TlFVRk5MRU5CUVVNc1UwRkJVeXhIUVVGSExGbEJRVmtzUTBGQlF5eEpRVUZKTEVOQlFVTXNVMEZCVXl4RFFVRkRMRWxCUVVrc1EwRkJReXhKUVVGSkxFTkJRVU1zUTBGQlF5eERRVUZETzFOQlF6VkVPMUZCUlVRc1QwRkJUeXhOUVVGTkxFTkJRVU03U1VGRGFFSXNRMEZCUXp0SlFVTm5RaXhaUVVGWkxFTkJRVU03U1VGRGRFSXNhVUpCUVdsQ0xFTkJRVU03U1VGRGJFSXNWVUZCVlN4SFFVRlpMRXRCUVVzc1EwRkJRenRKUVVNMVFpeGpRVUZqTEVkQlFUQkNMRVZCUVVVc1EwRkJRenRKUVVNelF5eFBRVUZQTEVOQlFVTTdTVUZGYUVJc1dVRkJXU3haUVVGWk8xRkJRM1JDTEVsQlFVa3NRMEZCUXl4WlFVRlpMRWRCUVVjc1dVRkJXU3hEUVVGRE8wbEJRMjVETEVOQlFVTTdTVUZGUkRzN1QwRkZSenRKUVVOSkxFMUJRVTA3VVVGRFdDeEpRVUZKTEVOQlFVTXNhVUpCUVdsQ0xFZEJRVWNzUTBGQlF5eFBRVUZQTEVWQlFVVXNUVUZCVFN4RlFVRkZMRVZCUVVVN1dVRkRNME1zU1VGRFJTeFBRVUZQTEVOQlFVTXNVMEZCVXp0blFrRkRha0lzVDBGQlR5eERRVUZETEZOQlFWTXNTMEZCU3l3MFFrRkJORUlzUlVGRGJFUTdaMEpCUTBFc1NVRkJTU3hEUVVGRExEaENRVUU0UWl4RFFVRkRMRTlCUVU4c1JVRkJSU3hOUVVGTkxFTkJRVU1zUTBGQlF6dGhRVU4wUkR0UlFVTklMRU5CUVVNc1EwRkJRenRSUVVOR0xFOUJRVThzUTBGQlF5eFBRVUZQTEVOQlFVTXNVMEZCVXl4RFFVRkRMRmRCUVZjc1EwRkJReXhKUVVGSkxFTkJRVU1zYVVKQlFXbENMRU5CUVVNc1EwRkJRenRKUVVOb1JTeERRVUZETzBsQlJVUTdPenM3T3p0UFFVMUhPMGxCUTBrc09FSkJRVGhDTEVOQlFVTXNUMEZCVHl4RlFVRkZMRTFCUVhGQ08xRkJRMnhGTEZGQlFWRXNUMEZCVHl4RFFVRkRMRWxCUVVrc1JVRkJSVHRaUVVOd1FpeExRVUZMTEZOQlFWTXNRMEZCUXp0WlFVTm1MRXRCUVVzc1ZVRkJWVHRuUWtGRFlpeE5RVUZOTEUxQlFVMHNSMEZCUnl4dlFrRkJiMElzUTBGQlF5eHhRa0ZCY1VJc1EwRkRka1FzVDBGQlR5eERRVUZETEVsQlFVa3NSVUZEV2l4TlFVRk5MRU5CUTFBc1EwRkJRenRuUWtGRFJpeEpRVUZKTEVsQlFVa3NRMEZCUXl4VlFVRlZMRVZCUVVVN2IwSkJRMjVDTEUxQlFVMHNRMEZCUXl4VlFVRlZMRWRCUVVjc1NVRkJTU3hEUVVGRExFOUJRVThzUTBGQlF6dHZRa0ZEYWtNc1NVRkJTU3hEUVVGRExGbEJRVmtzUTBGQlF5eFZRVUZWTEVOQlFVTXNXVUZCV1N4RlFVRkZMRTFCUVUwc1EwRkJReXhEUVVGRE8ybENRVU53UkR0eFFrRkJUVHR2UWtGRFRDeEpRVUZKTEVOQlFVTXNZMEZCWXl4RFFVRkRMRWxCUVVrc1EwRkJReXhOUVVGTkxFTkJRVU1zUTBGQlF6dHBRa0ZEYkVNN1owSkJRMFFzVFVGQlRUdFRRVU5VTzBsQlEwZ3NRMEZCUXp0SlFVVkVPenM3T3pzN1QwRk5SenRKUVVOSkxFZEJRVWNzUTBGQlF5eFBRVUZQTzFGQlEyaENMRWxCUVVrc1EwRkJReXhKUVVGSkxFTkJRVU1zYVVKQlFXbENMRVZCUVVVN1dVRkRNMElzU1VGQlNTeERRVUZETEUxQlFVMHNSVUZCUlN4RFFVRkRPMU5CUTJZN1VVRkRSQ3hKUVVGSkxFTkJRVU1zVDBGQlR5eEhRVUZITEU5QlFVOHNRMEZCUXp0UlFVTjJRaXhKUVVGSkxFTkJRVU1zVlVGQlZTeEhRVUZITEVsQlFVa3NRMEZCUXp0UlFVTjJRaXhKUVVGSkxFTkJRVU1zWTBGQll5eERRVUZETEVkQlFVY3NRMEZCUXl4RFFVRkRMRTFCUVUwc1JVRkJSU3hGUVVGRk8xbEJRMnBETEUxQlFVMHNRMEZCUXl4VlFVRlZMRWRCUVVjc1NVRkJTU3hEUVVGRExFOUJRVThzUTBGQlF6dFpRVU5xUXl4SlFVRkpMRU5CUVVNc1dVRkJXU3hEUVVGRExGVkJRVlVzUTBGQlF5eFpRVUZaTEVWQlFVVXNUVUZCVFN4RFFVRkRMRU5CUVVNN1VVRkRja1FzUTBGQlF5eERRVUZETEVOQlFVTTdTVUZEVEN4RFFVRkRPMGxCUlUwc1MwRkJTeXhEUVVGRExIRkNRVUZ4UWl4RFFVTm9ReXhQUVVGblFpeEZRVU5vUWl4NVFrRkJaMFE3VVVGRmFFUXNUVUZCVFN4dFFrRkJiVUlzUjBGQlJ6dFpRVU14UWl4UFFVRlBPMWxCUTFBc2VVSkJRWGxDTzFOQlF6RkNMRU5CUVVNN1VVRkRSaXhKUVVGSkxHMUNRVUZ0UWl4RlFVRkZPMWxCUTNaQ0xEWkVRVUUyUkR0WlFVTTNSQ3hOUVVGTkxFOUJRVThzUTBGQlF5eGpRVUZqTEVOQlFVTXNVVUZCVVN4RFFVRkRPMmRDUVVOd1F5eEZRVUZGTEVWQlFVVTdiMEpCUTBZN2QwSkJRMFVzU1VGQlNTeEZRVUZGTEhWRFFVRjFReXhKUVVGSkxFTkJRVU1zVTBGQlV5eERRVU42UkN4dFFrRkJiVUlzUTBGRGNFSXNSMEZCUnp0eFFrRkRURHRwUWtGRFJqdG5Ra0ZEUkN4UFFVRlBMRVZCUVVVc1EwRkJReXhaUVVGWkxFTkJRVU03WjBKQlEzWkNMRk5CUVZNc1JVRkJSU3hKUVVGSk8yZENRVU5tTEV0QlFVc3NSVUZCUlN4blFrRkJaMEk3WjBKQlEzWkNMR1ZCUVdVc1JVRkJSU3hKUVVGSk8yRkJRM1JDTEVOQlFVTXNRMEZCUXp0VFFVTktPMUZCUTBRc1QwRkJUeXhQUVVGUExFTkJRVU1zWTBGQll5eERRVUZETEZGQlFWRXNRMEZCUXp0WlFVTnlReXhGUVVGRkxFVkJRVVVzUTBGQlF5eEZRVUZGTEVsQlFVa3NSVUZCUlN4aFFVRmhMRVZCUVVVc1EwRkJRenRaUVVNM1FpeFBRVUZQTEVWQlFVVXNRMEZCUXl4WlFVRlpMRU5CUVVNN1dVRkRka0lzVTBGQlV5eEZRVUZGTEVsQlFVazdXVUZEWml4TFFVRkxMRVZCUVVVc1owSkJRV2RDTzFsQlEzWkNMR1ZCUVdVc1JVRkJSU3hKUVVGSk8xTkJRM1JDTEVOQlFVTXNRMEZCUXp0SlFVTk1MRU5CUVVNN1NVRkZUU3hQUVVGUE8xRkJRMW9zU1VGQlNTeERRVUZETEdOQlFXTXNSMEZCUnl4RlFVRkZMRU5CUVVNN1VVRkRla0lzU1VGQlNTeEpRVUZKTEVOQlFVTXNhVUpCUVdsQ0xFVkJRVVU3V1VGRE1VSXNUMEZCVHl4RFFVRkRMRTlCUVU4c1EwRkJReXhUUVVGVExFTkJRVU1zWTBGQll5eERRVUZETEVsQlFVa3NRMEZCUXl4cFFrRkJhVUlzUTBGQlF5eERRVUZETzFOQlEyeEZPMGxCUTBnc1EwRkJRenREUVVOR0luMD0iLCJpbXBvcnQgeyBpbmNyZW1lbnRlZEV2ZW50T3JkaW5hbCB9IGZyb20gXCIuLi9saWIvZXh0ZW5zaW9uLXNlc3Npb24tZXZlbnQtb3JkaW5hbFwiO1xuaW1wb3J0IHsgZXh0ZW5zaW9uU2Vzc2lvblV1aWQgfSBmcm9tIFwiLi4vbGliL2V4dGVuc2lvbi1zZXNzaW9uLXV1aWRcIjtcbmltcG9ydCB7IFBlbmRpbmdOYXZpZ2F0aW9uIH0gZnJvbSBcIi4uL2xpYi9wZW5kaW5nLW5hdmlnYXRpb25cIjtcbmltcG9ydCB7IGJvb2xUb0ludCwgZXNjYXBlU3RyaW5nLCBlc2NhcGVVcmwgfSBmcm9tIFwiLi4vbGliL3N0cmluZy11dGlsc1wiO1xuaW1wb3J0IHsgbWFrZVVVSUQgfSBmcm9tIFwiLi4vbGliL3V1aWRcIjtcbmV4cG9ydCBjb25zdCB0cmFuc2Zvcm1XZWJOYXZpZ2F0aW9uQmFzZUV2ZW50RGV0YWlsc1RvT3BlbldQTVNjaGVtYSA9IGFzeW5jIChjcmF3bElELCBkZXRhaWxzKSA9PiB7XG4gICAgY29uc3QgdGFiID0gZGV0YWlscy50YWJJZCA+IC0xXG4gICAgICAgID8gYXdhaXQgYnJvd3Nlci50YWJzLmdldChkZXRhaWxzLnRhYklkKVxuICAgICAgICA6IHtcbiAgICAgICAgICAgIHdpbmRvd0lkOiB1bmRlZmluZWQsXG4gICAgICAgICAgICBpbmNvZ25pdG86IHVuZGVmaW5lZCxcbiAgICAgICAgICAgIGNvb2tpZVN0b3JlSWQ6IHVuZGVmaW5lZCxcbiAgICAgICAgICAgIG9wZW5lclRhYklkOiB1bmRlZmluZWQsXG4gICAgICAgICAgICB3aWR0aDogdW5kZWZpbmVkLFxuICAgICAgICAgICAgaGVpZ2h0OiB1bmRlZmluZWQsXG4gICAgICAgIH07XG4gICAgY29uc3Qgd2luZG93ID0gdGFiLndpbmRvd0lkXG4gICAgICAgID8gYXdhaXQgYnJvd3Nlci53aW5kb3dzLmdldCh0YWIud2luZG93SWQpXG4gICAgICAgIDogeyB3aWR0aDogdW5kZWZpbmVkLCBoZWlnaHQ6IHVuZGVmaW5lZCwgdHlwZTogdW5kZWZpbmVkIH07XG4gICAgY29uc3QgbmF2aWdhdGlvbiA9IHtcbiAgICAgICAgYnJvd3Nlcl9pZDogY3Jhd2xJRCxcbiAgICAgICAgaW5jb2duaXRvOiBib29sVG9JbnQodGFiLmluY29nbml0byksXG4gICAgICAgIGV4dGVuc2lvbl9zZXNzaW9uX3V1aWQ6IGV4dGVuc2lvblNlc3Npb25VdWlkLFxuICAgICAgICBwcm9jZXNzX2lkOiBkZXRhaWxzLnByb2Nlc3NJZCxcbiAgICAgICAgd2luZG93X2lkOiB0YWIud2luZG93SWQsXG4gICAgICAgIHRhYl9pZDogZGV0YWlscy50YWJJZCxcbiAgICAgICAgdGFiX29wZW5lcl90YWJfaWQ6IHRhYi5vcGVuZXJUYWJJZCxcbiAgICAgICAgZnJhbWVfaWQ6IGRldGFpbHMuZnJhbWVJZCxcbiAgICAgICAgd2luZG93X3dpZHRoOiB3aW5kb3cud2lkdGgsXG4gICAgICAgIHdpbmRvd19oZWlnaHQ6IHdpbmRvdy5oZWlnaHQsXG4gICAgICAgIHdpbmRvd190eXBlOiB3aW5kb3cudHlwZSxcbiAgICAgICAgdGFiX3dpZHRoOiB0YWIud2lkdGgsXG4gICAgICAgIHRhYl9oZWlnaHQ6IHRhYi5oZWlnaHQsXG4gICAgICAgIHRhYl9jb29raWVfc3RvcmVfaWQ6IGVzY2FwZVN0cmluZyh0YWIuY29va2llU3RvcmVJZCksXG4gICAgICAgIHV1aWQ6IG1ha2VVVUlEKCksXG4gICAgICAgIHVybDogZXNjYXBlVXJsKGRldGFpbHMudXJsKSxcbiAgICB9O1xuICAgIHJldHVybiBuYXZpZ2F0aW9uO1xufTtcbmV4cG9ydCBjbGFzcyBOYXZpZ2F0aW9uSW5zdHJ1bWVudCB7XG4gICAgc3RhdGljIG5hdmlnYXRpb25JZChwcm9jZXNzSWQsIHRhYklkLCBmcmFtZUlkKSB7XG4gICAgICAgIHJldHVybiBgJHtwcm9jZXNzSWR9LSR7dGFiSWR9LSR7ZnJhbWVJZH1gO1xuICAgIH1cbiAgICBkYXRhUmVjZWl2ZXI7XG4gICAgb25CZWZvcmVOYXZpZ2F0ZUxpc3RlbmVyO1xuICAgIG9uQ29tbWl0dGVkTGlzdGVuZXI7XG4gICAgcGVuZGluZ05hdmlnYXRpb25zID0ge307XG4gICAgY29uc3RydWN0b3IoZGF0YVJlY2VpdmVyKSB7XG4gICAgICAgIHRoaXMuZGF0YVJlY2VpdmVyID0gZGF0YVJlY2VpdmVyO1xuICAgIH1cbiAgICBydW4oY3Jhd2xJRCkge1xuICAgICAgICB0aGlzLm9uQmVmb3JlTmF2aWdhdGVMaXN0ZW5lciA9IGFzeW5jIChkZXRhaWxzKSA9PiB7XG4gICAgICAgICAgICBjb25zdCBuYXZpZ2F0aW9uSWQgPSBOYXZpZ2F0aW9uSW5zdHJ1bWVudC5uYXZpZ2F0aW9uSWQoZGV0YWlscy5wcm9jZXNzSWQsIGRldGFpbHMudGFiSWQsIGRldGFpbHMuZnJhbWVJZCk7XG4gICAgICAgICAgICBjb25zdCBwZW5kaW5nTmF2aWdhdGlvbiA9IHRoaXMuaW5zdGFudGlhdGVQZW5kaW5nTmF2aWdhdGlvbihuYXZpZ2F0aW9uSWQpO1xuICAgICAgICAgICAgY29uc3QgbmF2aWdhdGlvbiA9IGF3YWl0IHRyYW5zZm9ybVdlYk5hdmlnYXRpb25CYXNlRXZlbnREZXRhaWxzVG9PcGVuV1BNU2NoZW1hKGNyYXdsSUQsIGRldGFpbHMpO1xuICAgICAgICAgICAgbmF2aWdhdGlvbi5wYXJlbnRfZnJhbWVfaWQgPSBkZXRhaWxzLnBhcmVudEZyYW1lSWQ7XG4gICAgICAgICAgICBuYXZpZ2F0aW9uLmJlZm9yZV9uYXZpZ2F0ZV9ldmVudF9vcmRpbmFsID0gaW5jcmVtZW50ZWRFdmVudE9yZGluYWwoKTtcbiAgICAgICAgICAgIG5hdmlnYXRpb24uYmVmb3JlX25hdmlnYXRlX3RpbWVfc3RhbXAgPSBuZXcgRGF0ZShkZXRhaWxzLnRpbWVTdGFtcCkudG9JU09TdHJpbmcoKTtcbiAgICAgICAgICAgIHBlbmRpbmdOYXZpZ2F0aW9uLnJlc29sdmVPbkJlZm9yZU5hdmlnYXRlRXZlbnROYXZpZ2F0aW9uKG5hdmlnYXRpb24pO1xuICAgICAgICB9O1xuICAgICAgICBicm93c2VyLndlYk5hdmlnYXRpb24ub25CZWZvcmVOYXZpZ2F0ZS5hZGRMaXN0ZW5lcih0aGlzLm9uQmVmb3JlTmF2aWdhdGVMaXN0ZW5lcik7XG4gICAgICAgIHRoaXMub25Db21taXR0ZWRMaXN0ZW5lciA9IGFzeW5jIChkZXRhaWxzKSA9PiB7XG4gICAgICAgICAgICBjb25zdCBuYXZpZ2F0aW9uSWQgPSBOYXZpZ2F0aW9uSW5zdHJ1bWVudC5uYXZpZ2F0aW9uSWQoZGV0YWlscy5wcm9jZXNzSWQsIGRldGFpbHMudGFiSWQsIGRldGFpbHMuZnJhbWVJZCk7XG4gICAgICAgICAgICBjb25zdCBuYXZpZ2F0aW9uID0gYXdhaXQgdHJhbnNmb3JtV2ViTmF2aWdhdGlvbkJhc2VFdmVudERldGFpbHNUb09wZW5XUE1TY2hlbWEoY3Jhd2xJRCwgZGV0YWlscyk7XG4gICAgICAgICAgICBuYXZpZ2F0aW9uLnRyYW5zaXRpb25fcXVhbGlmaWVycyA9IGVzY2FwZVN0cmluZyhKU09OLnN0cmluZ2lmeShkZXRhaWxzLnRyYW5zaXRpb25RdWFsaWZpZXJzKSk7XG4gICAgICAgICAgICBuYXZpZ2F0aW9uLnRyYW5zaXRpb25fdHlwZSA9IGVzY2FwZVN0cmluZyhkZXRhaWxzLnRyYW5zaXRpb25UeXBlKTtcbiAgICAgICAgICAgIG5hdmlnYXRpb24uY29tbWl0dGVkX2V2ZW50X29yZGluYWwgPSBpbmNyZW1lbnRlZEV2ZW50T3JkaW5hbCgpO1xuICAgICAgICAgICAgbmF2aWdhdGlvbi5jb21taXR0ZWRfdGltZV9zdGFtcCA9IG5ldyBEYXRlKGRldGFpbHMudGltZVN0YW1wKS50b0lTT1N0cmluZygpO1xuICAgICAgICAgICAgLy8gaW5jbHVkZSBhdHRyaWJ1dGVzIGZyb20gdGhlIGNvcnJlc3BvbmRpbmcgb25CZWZvcmVOYXZpZ2F0aW9uIGV2ZW50XG4gICAgICAgICAgICBjb25zdCBwZW5kaW5nTmF2aWdhdGlvbiA9IHRoaXMuZ2V0UGVuZGluZ05hdmlnYXRpb24obmF2aWdhdGlvbklkKTtcbiAgICAgICAgICAgIGlmIChwZW5kaW5nTmF2aWdhdGlvbikge1xuICAgICAgICAgICAgICAgIHBlbmRpbmdOYXZpZ2F0aW9uLnJlc29sdmVPbkNvbW1pdHRlZEV2ZW50TmF2aWdhdGlvbihuYXZpZ2F0aW9uKTtcbiAgICAgICAgICAgICAgICBjb25zdCByZXNvbHZlZCA9IGF3YWl0IHBlbmRpbmdOYXZpZ2F0aW9uLnJlc29sdmVkV2l0aGluVGltZW91dCgxMDAwKTtcbiAgICAgICAgICAgICAgICBpZiAocmVzb2x2ZWQpIHtcbiAgICAgICAgICAgICAgICAgICAgY29uc3Qgb25CZWZvcmVOYXZpZ2F0ZUV2ZW50TmF2aWdhdGlvbiA9IGF3YWl0IHBlbmRpbmdOYXZpZ2F0aW9uLm9uQmVmb3JlTmF2aWdhdGVFdmVudE5hdmlnYXRpb247XG4gICAgICAgICAgICAgICAgICAgIG5hdmlnYXRpb24ucGFyZW50X2ZyYW1lX2lkID1cbiAgICAgICAgICAgICAgICAgICAgICAgIG9uQmVmb3JlTmF2aWdhdGVFdmVudE5hdmlnYXRpb24ucGFyZW50X2ZyYW1lX2lkO1xuICAgICAgICAgICAgICAgICAgICBuYXZpZ2F0aW9uLmJlZm9yZV9uYXZpZ2F0ZV9ldmVudF9vcmRpbmFsID1cbiAgICAgICAgICAgICAgICAgICAgICAgIG9uQmVmb3JlTmF2aWdhdGVFdmVudE5hdmlnYXRpb24uYmVmb3JlX25hdmlnYXRlX2V2ZW50X29yZGluYWw7XG4gICAgICAgICAgICAgICAgICAgIG5hdmlnYXRpb24uYmVmb3JlX25hdmlnYXRlX3RpbWVfc3RhbXAgPVxuICAgICAgICAgICAgICAgICAgICAgICAgb25CZWZvcmVOYXZpZ2F0ZUV2ZW50TmF2aWdhdGlvbi5iZWZvcmVfbmF2aWdhdGVfdGltZV9zdGFtcDtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICB0aGlzLmRhdGFSZWNlaXZlci5zYXZlUmVjb3JkKFwibmF2aWdhdGlvbnNcIiwgbmF2aWdhdGlvbik7XG4gICAgICAgIH07XG4gICAgICAgIGJyb3dzZXIud2ViTmF2aWdhdGlvbi5vbkNvbW1pdHRlZC5hZGRMaXN0ZW5lcih0aGlzLm9uQ29tbWl0dGVkTGlzdGVuZXIpO1xuICAgIH1cbiAgICBjbGVhbnVwKCkge1xuICAgICAgICBpZiAodGhpcy5vbkJlZm9yZU5hdmlnYXRlTGlzdGVuZXIpIHtcbiAgICAgICAgICAgIGJyb3dzZXIud2ViTmF2aWdhdGlvbi5vbkJlZm9yZU5hdmlnYXRlLnJlbW92ZUxpc3RlbmVyKHRoaXMub25CZWZvcmVOYXZpZ2F0ZUxpc3RlbmVyKTtcbiAgICAgICAgfVxuICAgICAgICBpZiAodGhpcy5vbkNvbW1pdHRlZExpc3RlbmVyKSB7XG4gICAgICAgICAgICBicm93c2VyLndlYk5hdmlnYXRpb24ub25Db21taXR0ZWQucmVtb3ZlTGlzdGVuZXIodGhpcy5vbkNvbW1pdHRlZExpc3RlbmVyKTtcbiAgICAgICAgfVxuICAgIH1cbiAgICBpbnN0YW50aWF0ZVBlbmRpbmdOYXZpZ2F0aW9uKG5hdmlnYXRpb25JZCkge1xuICAgICAgICB0aGlzLnBlbmRpbmdOYXZpZ2F0aW9uc1tuYXZpZ2F0aW9uSWRdID0gbmV3IFBlbmRpbmdOYXZpZ2F0aW9uKCk7XG4gICAgICAgIHJldHVybiB0aGlzLnBlbmRpbmdOYXZpZ2F0aW9uc1tuYXZpZ2F0aW9uSWRdO1xuICAgIH1cbiAgICBnZXRQZW5kaW5nTmF2aWdhdGlvbihuYXZpZ2F0aW9uSWQpIHtcbiAgICAgICAgcmV0dXJuIHRoaXMucGVuZGluZ05hdmlnYXRpb25zW25hdmlnYXRpb25JZF07XG4gICAgfVxufVxuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pYm1GMmFXZGhkR2x2YmkxcGJuTjBjblZ0Wlc1MExtcHpJaXdpYzI5MWNtTmxVbTl2ZENJNklpSXNJbk52ZFhKalpYTWlPbHNpTGk0dkxpNHZMaTR2YzNKakwySmhZMnRuY205MWJtUXZibUYyYVdkaGRHbHZiaTFwYm5OMGNuVnRaVzUwTG5SeklsMHNJbTVoYldWeklqcGJYU3dpYldGd2NHbHVaM01pT2lKQlFVRkJMRTlCUVU4c1JVRkJSU3gxUWtGQmRVSXNSVUZCUlN4TlFVRk5MSGREUVVGM1F5eERRVUZETzBGQlEycEdMRTlCUVU4c1JVRkJSU3h2UWtGQmIwSXNSVUZCUlN4TlFVRk5MQ3RDUVVFclFpeERRVUZETzBGQlEzSkZMRTlCUVU4c1JVRkJSU3hwUWtGQmFVSXNSVUZCUlN4TlFVRk5MREpDUVVFeVFpeERRVUZETzBGQlF6bEVMRTlCUVU4c1JVRkJSU3hUUVVGVExFVkJRVVVzV1VGQldTeEZRVUZGTEZOQlFWTXNSVUZCUlN4TlFVRk5MSEZDUVVGeFFpeERRVUZETzBGQlEzcEZMRTlCUVU4c1JVRkJSU3hSUVVGUkxFVkJRVVVzVFVGQlRTeGhRVUZoTEVOQlFVTTdRVUZSZGtNc1RVRkJUU3hEUVVGRExFMUJRVTBzY1VSQlFYRkVMRWRCUVVjc1MwRkJTeXhGUVVONFJTeFBRVUZQTEVWQlExQXNUMEZCYzBNc1JVRkRha0lzUlVGQlJUdEpRVU4yUWl4TlFVRk5MRWRCUVVjc1IwRkRVQ3hQUVVGUExFTkJRVU1zUzBGQlN5eEhRVUZITEVOQlFVTXNRMEZCUXp0UlFVTm9RaXhEUVVGRExFTkJRVU1zVFVGQlRTeFBRVUZQTEVOQlFVTXNTVUZCU1N4RFFVRkRMRWRCUVVjc1EwRkJReXhQUVVGUExFTkJRVU1zUzBGQlN5eERRVUZETzFGQlEzWkRMRU5CUVVNc1EwRkJRenRaUVVORkxGRkJRVkVzUlVGQlJTeFRRVUZUTzFsQlEyNUNMRk5CUVZNc1JVRkJSU3hUUVVGVE8xbEJRM0JDTEdGQlFXRXNSVUZCUlN4VFFVRlRPMWxCUTNoQ0xGZEJRVmNzUlVGQlJTeFRRVUZUTzFsQlEzUkNMRXRCUVVzc1JVRkJSU3hUUVVGVE8xbEJRMmhDTEUxQlFVMHNSVUZCUlN4VFFVRlRPMU5CUTJ4Q0xFTkJRVU03U1VGRFVpeE5RVUZOTEUxQlFVMHNSMEZCUnl4SFFVRkhMRU5CUVVNc1VVRkJVVHRSUVVONlFpeERRVUZETEVOQlFVTXNUVUZCVFN4UFFVRlBMRU5CUVVNc1QwRkJUeXhEUVVGRExFZEJRVWNzUTBGQlF5eEhRVUZITEVOQlFVTXNVVUZCVVN4RFFVRkRPMUZCUTNwRExFTkJRVU1zUTBGQlF5eEZRVUZGTEV0QlFVc3NSVUZCUlN4VFFVRlRMRVZCUVVVc1RVRkJUU3hGUVVGRkxGTkJRVk1zUlVGQlJTeEpRVUZKTEVWQlFVVXNVMEZCVXl4RlFVRkZMRU5CUVVNN1NVRkROMFFzVFVGQlRTeFZRVUZWTEVkQlFXVTdVVUZETjBJc1ZVRkJWU3hGUVVGRkxFOUJRVTg3VVVGRGJrSXNVMEZCVXl4RlFVRkZMRk5CUVZNc1EwRkJReXhIUVVGSExFTkJRVU1zVTBGQlV5eERRVUZETzFGQlEyNURMSE5DUVVGelFpeEZRVUZGTEc5Q1FVRnZRanRSUVVNMVF5eFZRVUZWTEVWQlFVVXNUMEZCVHl4RFFVRkRMRk5CUVZNN1VVRkROMElzVTBGQlV5eEZRVUZGTEVkQlFVY3NRMEZCUXl4UlFVRlJPMUZCUTNaQ0xFMUJRVTBzUlVGQlJTeFBRVUZQTEVOQlFVTXNTMEZCU3p0UlFVTnlRaXhwUWtGQmFVSXNSVUZCUlN4SFFVRkhMRU5CUVVNc1YwRkJWenRSUVVOc1F5eFJRVUZSTEVWQlFVVXNUMEZCVHl4RFFVRkRMRTlCUVU4N1VVRkRla0lzV1VGQldTeEZRVUZGTEUxQlFVMHNRMEZCUXl4TFFVRkxPMUZCUXpGQ0xHRkJRV0VzUlVGQlJTeE5RVUZOTEVOQlFVTXNUVUZCVFR0UlFVTTFRaXhYUVVGWExFVkJRVVVzVFVGQlRTeERRVUZETEVsQlFVazdVVUZEZUVJc1UwRkJVeXhGUVVGRkxFZEJRVWNzUTBGQlF5eExRVUZMTzFGQlEzQkNMRlZCUVZVc1JVRkJSU3hIUVVGSExFTkJRVU1zVFVGQlRUdFJRVU4wUWl4dFFrRkJiVUlzUlVGQlJTeFpRVUZaTEVOQlFVTXNSMEZCUnl4RFFVRkRMR0ZCUVdFc1EwRkJRenRSUVVOd1JDeEpRVUZKTEVWQlFVVXNVVUZCVVN4RlFVRkZPMUZCUTJoQ0xFZEJRVWNzUlVGQlJTeFRRVUZUTEVOQlFVTXNUMEZCVHl4RFFVRkRMRWRCUVVjc1EwRkJRenRMUVVNMVFpeERRVUZETzBsQlEwWXNUMEZCVHl4VlFVRlZMRU5CUVVNN1FVRkRjRUlzUTBGQlF5eERRVUZETzBGQlJVWXNUVUZCVFN4UFFVRlBMRzlDUVVGdlFqdEpRVU40UWl4TlFVRk5MRU5CUVVNc1dVRkJXU3hEUVVGRExGTkJRVk1zUlVGQlJTeExRVUZMTEVWQlFVVXNUMEZCVHp0UlFVTnNSQ3hQUVVGUExFZEJRVWNzVTBGQlV5eEpRVUZKTEV0QlFVc3NTVUZCU1N4UFFVRlBMRVZCUVVVc1EwRkJRenRKUVVNMVF5eERRVUZETzBsQlEyZENMRmxCUVZrc1EwRkJRenRKUVVOMFFpeDNRa0ZCZDBJc1EwRkJRenRKUVVONlFpeHRRa0ZCYlVJc1EwRkJRenRKUVVOd1FpeHJRa0ZCYTBJc1IwRkZkRUlzUlVGQlJTeERRVUZETzBsQlJWQXNXVUZCV1N4WlFVRlpPMUZCUTNSQ0xFbEJRVWtzUTBGQlF5eFpRVUZaTEVkQlFVY3NXVUZCV1N4RFFVRkRPMGxCUTI1RExFTkJRVU03U1VGRlRTeEhRVUZITEVOQlFVTXNUMEZCVHp0UlFVTm9RaXhKUVVGSkxFTkJRVU1zZDBKQlFYZENMRWRCUVVjc1MwRkJTeXhGUVVOdVF5eFBRVUZyUkN4RlFVTnNSQ3hGUVVGRk8xbEJRMFlzVFVGQlRTeFpRVUZaTEVkQlFVY3NiMEpCUVc5Q0xFTkJRVU1zV1VGQldTeERRVU53UkN4UFFVRlBMRU5CUVVNc1UwRkJVeXhGUVVOcVFpeFBRVUZQTEVOQlFVTXNTMEZCU3l4RlFVTmlMRTlCUVU4c1EwRkJReXhQUVVGUExFTkJRMmhDTEVOQlFVTTdXVUZEUml4TlFVRk5MR2xDUVVGcFFpeEhRVUZITEVsQlFVa3NRMEZCUXl3MFFrRkJORUlzUTBGQlF5eFpRVUZaTEVOQlFVTXNRMEZCUXp0WlFVTXhSU3hOUVVGTkxGVkJRVlVzUjBGRFpDeE5RVUZOTEhGRVFVRnhSQ3hEUVVONlJDeFBRVUZQTEVWQlExQXNUMEZCVHl4RFFVTlNMRU5CUVVNN1dVRkRTaXhWUVVGVkxFTkJRVU1zWlVGQlpTeEhRVUZITEU5QlFVOHNRMEZCUXl4aFFVRmhMRU5CUVVNN1dVRkRia1FzVlVGQlZTeERRVUZETERaQ1FVRTJRaXhIUVVGSExIVkNRVUYxUWl4RlFVRkZMRU5CUVVNN1dVRkRja1VzVlVGQlZTeERRVUZETERCQ1FVRXdRaXhIUVVGSExFbEJRVWtzU1VGQlNTeERRVU01UXl4UFFVRlBMRU5CUVVNc1UwRkJVeXhEUVVOc1FpeERRVUZETEZkQlFWY3NSVUZCUlN4RFFVRkRPMWxCUTJoQ0xHbENRVUZwUWl4RFFVRkRMSE5EUVVGelF5eERRVUZETEZWQlFWVXNRMEZCUXl4RFFVRkRPMUZCUTNaRkxFTkJRVU1zUTBGQlF6dFJRVU5HTEU5QlFVOHNRMEZCUXl4aFFVRmhMRU5CUVVNc1owSkJRV2RDTEVOQlFVTXNWMEZCVnl4RFFVTm9SQ3hKUVVGSkxFTkJRVU1zZDBKQlFYZENMRU5CUXpsQ0xFTkJRVU03VVVGRFJpeEpRVUZKTEVOQlFVTXNiVUpCUVcxQ0xFZEJRVWNzUzBGQlN5eEZRVU01UWl4UFFVRTJReXhGUVVNM1F5eEZRVUZGTzFsQlEwWXNUVUZCVFN4WlFVRlpMRWRCUVVjc2IwSkJRVzlDTEVOQlFVTXNXVUZCV1N4RFFVTndSQ3hQUVVGUExFTkJRVU1zVTBGQlV5eEZRVU5xUWl4UFFVRlBMRU5CUVVNc1MwRkJTeXhGUVVOaUxFOUJRVThzUTBGQlF5eFBRVUZQTEVOQlEyaENMRU5CUVVNN1dVRkRSaXhOUVVGTkxGVkJRVlVzUjBGRFpDeE5RVUZOTEhGRVFVRnhSQ3hEUVVONlJDeFBRVUZQTEVWQlExQXNUMEZCVHl4RFFVTlNMRU5CUVVNN1dVRkRTaXhWUVVGVkxFTkJRVU1zY1VKQlFYRkNMRWRCUVVjc1dVRkJXU3hEUVVNM1F5eEpRVUZKTEVOQlFVTXNVMEZCVXl4RFFVRkRMRTlCUVU4c1EwRkJReXh2UWtGQmIwSXNRMEZCUXl4RFFVTTNReXhEUVVGRE8xbEJRMFlzVlVGQlZTeERRVUZETEdWQlFXVXNSMEZCUnl4WlFVRlpMRU5CUVVNc1QwRkJUeXhEUVVGRExHTkJRV01zUTBGQlF5eERRVUZETzFsQlEyeEZMRlZCUVZVc1EwRkJReXgxUWtGQmRVSXNSMEZCUnl4MVFrRkJkVUlzUlVGQlJTeERRVUZETzFsQlF5OUVMRlZCUVZVc1EwRkJReXh2UWtGQmIwSXNSMEZCUnl4SlFVRkpMRWxCUVVrc1EwRkRlRU1zVDBGQlR5eERRVUZETEZOQlFWTXNRMEZEYkVJc1EwRkJReXhYUVVGWExFVkJRVVVzUTBGQlF6dFpRVVZvUWl4eFJVRkJjVVU3V1VGRGNrVXNUVUZCVFN4cFFrRkJhVUlzUjBGQlJ5eEpRVUZKTEVOQlFVTXNiMEpCUVc5Q0xFTkJRVU1zV1VGQldTeERRVUZETEVOQlFVTTdXVUZEYkVVc1NVRkJTU3hwUWtGQmFVSXNSVUZCUlR0blFrRkRja0lzYVVKQlFXbENMRU5CUVVNc2FVTkJRV2xETEVOQlFVTXNWVUZCVlN4RFFVRkRMRU5CUVVNN1owSkJRMmhGTEUxQlFVMHNVVUZCVVN4SFFVRkhMRTFCUVUwc2FVSkJRV2xDTEVOQlFVTXNjVUpCUVhGQ0xFTkJRVU1zU1VGQlNTeERRVUZETEVOQlFVTTdaMEpCUTNKRkxFbEJRVWtzVVVGQlVTeEZRVUZGTzI5Q1FVTmFMRTFCUVUwc0swSkJRU3RDTEVkQlEyNURMRTFCUVUwc2FVSkJRV2xDTEVOQlFVTXNLMEpCUVN0Q0xFTkJRVU03YjBKQlF6RkVMRlZCUVZVc1EwRkJReXhsUVVGbE8zZENRVU40UWl3clFrRkJLMElzUTBGQlF5eGxRVUZsTEVOQlFVTTdiMEpCUTJ4RUxGVkJRVlVzUTBGQlF5dzJRa0ZCTmtJN2QwSkJRM1JETEN0Q1FVRXJRaXhEUVVGRExEWkNRVUUyUWl4RFFVRkRPMjlDUVVOb1JTeFZRVUZWTEVOQlFVTXNNRUpCUVRCQ08zZENRVU51UXl3clFrRkJLMElzUTBGQlF5d3dRa0ZCTUVJc1EwRkJRenRwUWtGRE9VUTdZVUZEUmp0WlFVVkVMRWxCUVVrc1EwRkJReXhaUVVGWkxFTkJRVU1zVlVGQlZTeERRVUZETEdGQlFXRXNSVUZCUlN4VlFVRlZMRU5CUVVNc1EwRkJRenRSUVVNeFJDeERRVUZETEVOQlFVTTdVVUZEUml4UFFVRlBMRU5CUVVNc1lVRkJZU3hEUVVGRExGZEJRVmNzUTBGQlF5eFhRVUZYTEVOQlFVTXNTVUZCU1N4RFFVRkRMRzFDUVVGdFFpeERRVUZETEVOQlFVTTdTVUZETVVVc1EwRkJRenRKUVVWTkxFOUJRVTg3VVVGRFdpeEpRVUZKTEVsQlFVa3NRMEZCUXl4M1FrRkJkMElzUlVGQlJUdFpRVU5xUXl4UFFVRlBMRU5CUVVNc1lVRkJZU3hEUVVGRExHZENRVUZuUWl4RFFVRkRMR05CUVdNc1EwRkRia1FzU1VGQlNTeERRVUZETEhkQ1FVRjNRaXhEUVVNNVFpeERRVUZETzFOQlEwZzdVVUZEUkN4SlFVRkpMRWxCUVVrc1EwRkJReXh0UWtGQmJVSXNSVUZCUlR0WlFVTTFRaXhQUVVGUExFTkJRVU1zWVVGQllTeERRVUZETEZkQlFWY3NRMEZCUXl4alFVRmpMRU5CUXpsRExFbEJRVWtzUTBGQlF5eHRRa0ZCYlVJc1EwRkRla0lzUTBGQlF6dFRRVU5JTzBsQlEwZ3NRMEZCUXp0SlFVVlBMRFJDUVVFMFFpeERRVU5zUXl4WlFVRnZRanRSUVVWd1FpeEpRVUZKTEVOQlFVTXNhMEpCUVd0Q0xFTkJRVU1zV1VGQldTeERRVUZETEVkQlFVY3NTVUZCU1N4cFFrRkJhVUlzUlVGQlJTeERRVUZETzFGQlEyaEZMRTlCUVU4c1NVRkJTU3hEUVVGRExHdENRVUZyUWl4RFFVRkRMRmxCUVZrc1EwRkJReXhEUVVGRE8wbEJReTlETEVOQlFVTTdTVUZGVHl4dlFrRkJiMElzUTBGQlF5eFpRVUZ2UWp0UlFVTXZReXhQUVVGUExFbEJRVWtzUTBGQlF5eHJRa0ZCYTBJc1EwRkJReXhaUVVGWkxFTkJRVU1zUTBGQlF6dEpRVU12UXl4RFFVRkRPME5CUTBZaWZRPT0iLCJpbXBvcnQgeyBnZXRJbnN0cnVtZW50SlMgfSBmcm9tIFwiLi4vbGliL2pzLWluc3RydW1lbnRzXCI7XG5pbXBvcnQgeyBwYWdlU2NyaXB0IH0gZnJvbSBcIi4vamF2YXNjcmlwdC1pbnN0cnVtZW50LXBhZ2Utc2NvcGVcIjtcbmZ1bmN0aW9uIGdldFBhZ2VTY3JpcHRBc1N0cmluZyhqc0luc3RydW1lbnRhdGlvblNldHRpbmdzKSB7XG4gICAgLy8gVGhlIEpTIEluc3RydW1lbnQgUmVxdWVzdHMgYXJlIHNldHVwIGFuZCB2YWxpZGF0ZWQgcHl0aG9uIHNpZGVcbiAgICAvLyBpbmNsdWRpbmcgc2V0dGluZyBkZWZhdWx0cyBmb3IgbG9nU2V0dGluZ3MuIFNlZSBKU0luc3RydW1lbnRhdGlvbi5weVxuICAgIGNvbnN0IHBhZ2VTY3JpcHRTdHJpbmcgPSBgXG4vLyBTdGFydCBvZiBqcy1pbnN0cnVtZW50cy5cbiR7Z2V0SW5zdHJ1bWVudEpTfVxuLy8gRW5kIG9mIGpzLWluc3RydW1lbnRzLlxuXG4vLyBTdGFydCBvZiBjdXN0b20gaW5zdHJ1bWVudFJlcXVlc3RzLlxuY29uc3QganNJbnN0cnVtZW50YXRpb25TZXR0aW5ncyA9ICR7SlNPTi5zdHJpbmdpZnkoanNJbnN0cnVtZW50YXRpb25TZXR0aW5ncyl9O1xuLy8gRW5kIG9mIGN1c3RvbSBpbnN0cnVtZW50UmVxdWVzdHMuXG5cbi8vIFN0YXJ0IG9mIGFub255bW91cyBmdW5jdGlvbiBmcm9tIGphdmFzY3JpcHQtaW5zdHJ1bWVudC1wYWdlLXNjb3BlLnRzXG4oJHtwYWdlU2NyaXB0fShnZXRJbnN0cnVtZW50SlMsIGpzSW5zdHJ1bWVudGF0aW9uU2V0dGluZ3MpKTtcbi8vIEVuZC5cbiAgYDtcbiAgICByZXR1cm4gcGFnZVNjcmlwdFN0cmluZztcbn1cbjtcbmZ1bmN0aW9uIGluc2VydFNjcmlwdChwYWdlU2NyaXB0U3RyaW5nLCBldmVudElkLCB0ZXN0aW5nID0gZmFsc2UpIHtcbiAgICBjb25zdCBwYXJlbnQgPSBkb2N1bWVudC5kb2N1bWVudEVsZW1lbnQ7XG4gICAgY29uc3Qgc2NyaXB0ID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudChcInNjcmlwdFwiKTtcbiAgICBzY3JpcHQudGV4dCA9IHBhZ2VTY3JpcHRTdHJpbmc7XG4gICAgc2NyaXB0LmFzeW5jID0gZmFsc2U7XG4gICAgc2NyaXB0LnNldEF0dHJpYnV0ZShcImRhdGEtZXZlbnQtaWRcIiwgZXZlbnRJZCk7XG4gICAgc2NyaXB0LnNldEF0dHJpYnV0ZShcImRhdGEtdGVzdGluZ1wiLCBgJHt0ZXN0aW5nfWApO1xuICAgIHBhcmVudC5pbnNlcnRCZWZvcmUoc2NyaXB0LCBwYXJlbnQuZmlyc3RDaGlsZCk7XG4gICAgcGFyZW50LnJlbW92ZUNoaWxkKHNjcmlwdCk7XG59XG47XG5mdW5jdGlvbiBlbWl0TXNnKHR5cGUsIG1zZykge1xuICAgIG1zZy50aW1lU3RhbXAgPSBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCk7XG4gICAgYnJvd3Nlci5ydW50aW1lLnNlbmRNZXNzYWdlKHtcbiAgICAgICAgbmFtZXNwYWNlOiBcImphdmFzY3JpcHQtaW5zdHJ1bWVudGF0aW9uXCIsXG4gICAgICAgIHR5cGUsXG4gICAgICAgIGRhdGE6IG1zZyxcbiAgICB9KTtcbn1cbjtcbmNvbnN0IGV2ZW50SWQgPSBNYXRoLnJhbmRvbSgpLnRvU3RyaW5nKCk7XG4vLyBsaXN0ZW4gZm9yIG1lc3NhZ2VzIGZyb20gdGhlIHNjcmlwdCB3ZSBhcmUgYWJvdXQgdG8gaW5zZXJ0XG5kb2N1bWVudC5hZGRFdmVudExpc3RlbmVyKGV2ZW50SWQsIChlKSA9PiB7XG4gICAgLy8gcGFzcyB0aGVzZSBvbiB0byB0aGUgYmFja2dyb3VuZCBwYWdlXG4gICAgY29uc3QgbXNncyA9IGUuZGV0YWlsO1xuICAgIGlmIChBcnJheS5pc0FycmF5KG1zZ3MpKSB7XG4gICAgICAgIG1zZ3MuZm9yRWFjaCgobXNnKSA9PiB7XG4gICAgICAgICAgICBlbWl0TXNnKG1zZy50eXBlLCBtc2cuY29udGVudCk7XG4gICAgICAgIH0pO1xuICAgIH1cbiAgICBlbHNlIHtcbiAgICAgICAgZW1pdE1zZyhtc2dzLnR5cGUsIG1zZ3MuY29udGVudCk7XG4gICAgfVxufSk7XG5leHBvcnQgY29uc3QgaW5qZWN0SmF2YXNjcmlwdEluc3RydW1lbnRQYWdlU2NyaXB0ID0gKGNvbnRlbnRTY3JpcHRDb25maWcpID0+IHtcbiAgICBpbnNlcnRTY3JpcHQoZ2V0UGFnZVNjcmlwdEFzU3RyaW5nKGNvbnRlbnRTY3JpcHRDb25maWcuanNJbnN0cnVtZW50YXRpb25TZXR0aW5ncyksIGV2ZW50SWQsIGNvbnRlbnRTY3JpcHRDb25maWcudGVzdGluZyk7XG59O1xuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pYW1GMllYTmpjbWx3ZEMxcGJuTjBjblZ0Wlc1MExXTnZiblJsYm5RdGMyTnZjR1V1YW5NaUxDSnpiM1Z5WTJWU2IyOTBJam9pSWl3aWMyOTFjbU5sY3lJNld5SXVMaTh1TGk4dUxpOXpjbU12WTI5dWRHVnVkQzlxWVhaaGMyTnlhWEIwTFdsdWMzUnlkVzFsYm5RdFkyOXVkR1Z1ZEMxelkyOXdaUzUwY3lKZExDSnVZVzFsY3lJNlcxMHNJbTFoY0hCcGJtZHpJam9pUVVGQlFTeFBRVUZQTEVWQlFVVXNaVUZCWlN4RlFVRjFRaXhOUVVGTkxIVkNRVUYxUWl4RFFVRkRPMEZCUXpkRkxFOUJRVThzUlVGQlJTeFZRVUZWTEVWQlFVVXNUVUZCVFN4dlEwRkJiME1zUTBGQlF6dEJRVWRvUlN4VFFVRlRMSEZDUVVGeFFpeERRVU0xUWl4NVFrRkJaMFE3U1VGRmFFUXNhVVZCUVdsRk8wbEJRMnBGTEhWRlFVRjFSVHRKUVVOMlJTeE5RVUZOTEdkQ1FVRm5RaXhIUVVGSE96dEZRVVY2UWl4bFFVRmxPenM3TzI5RFFVbHRRaXhKUVVGSkxFTkJRVU1zVTBGQlV5eERRVUZETEhsQ1FVRjVRaXhEUVVGRE96czdPMGRCU1RGRkxGVkJRVlU3TzBkQlJWWXNRMEZCUXp0SlFVTkdMRTlCUVU4c1owSkJRV2RDTEVOQlFVTTdRVUZETVVJc1EwRkJRenRCUVVGQkxFTkJRVU03UVVGRlJpeFRRVUZUTEZsQlFWa3NRMEZEYmtJc1owSkJRWGRDTEVWQlEzaENMRTlCUVdVc1JVRkRaaXhWUVVGdFFpeExRVUZMTzBsQlJYaENMRTFCUVUwc1RVRkJUU3hIUVVGSExGRkJRVkVzUTBGQlF5eGxRVUZsTEVOQlFVTTdTVUZEZUVNc1RVRkJUU3hOUVVGTkxFZEJRVWNzVVVGQlVTeERRVUZETEdGQlFXRXNRMEZCUXl4UlFVRlJMRU5CUVVNc1EwRkJRenRKUVVOb1JDeE5RVUZOTEVOQlFVTXNTVUZCU1N4SFFVRkhMR2RDUVVGblFpeERRVUZETzBsQlF5OUNMRTFCUVUwc1EwRkJReXhMUVVGTExFZEJRVWNzUzBGQlN5eERRVUZETzBsQlEzSkNMRTFCUVUwc1EwRkJReXhaUVVGWkxFTkJRVU1zWlVGQlpTeEZRVUZGTEU5QlFVOHNRMEZCUXl4RFFVRkRPMGxCUXpsRExFMUJRVTBzUTBGQlF5eFpRVUZaTEVOQlFVTXNZMEZCWXl4RlFVRkZMRWRCUVVjc1QwRkJUeXhGUVVGRkxFTkJRVU1zUTBGQlF6dEpRVU5zUkN4TlFVRk5MRU5CUVVNc1dVRkJXU3hEUVVGRExFMUJRVTBzUlVGQlJTeE5RVUZOTEVOQlFVTXNWVUZCVlN4RFFVRkRMRU5CUVVNN1NVRkRMME1zVFVGQlRTeERRVUZETEZkQlFWY3NRMEZCUXl4TlFVRk5MRU5CUVVNc1EwRkJRenRCUVVNM1FpeERRVUZETzBGQlFVRXNRMEZCUXp0QlFVVkdMRk5CUVZNc1QwRkJUeXhEUVVGRkxFbEJRVWtzUlVGQlJTeEhRVUZITzBsQlEzcENMRWRCUVVjc1EwRkJReXhUUVVGVExFZEJRVWNzU1VGQlNTeEpRVUZKTEVWQlFVVXNRMEZCUXl4WFFVRlhMRVZCUVVVc1EwRkJRenRKUVVONlF5eFBRVUZQTEVOQlFVTXNUMEZCVHl4RFFVRkRMRmRCUVZjc1EwRkJRenRSUVVNeFFpeFRRVUZUTEVWQlFVVXNORUpCUVRSQ08xRkJRM1pETEVsQlFVazdVVUZEU2l4SlFVRkpMRVZCUVVVc1IwRkJSenRMUVVOV0xFTkJRVU1zUTBGQlF6dEJRVU5NTEVOQlFVTTdRVUZCUVN4RFFVRkRPMEZCUlVZc1RVRkJUU3hQUVVGUExFZEJRVWNzU1VGQlNTeERRVUZETEUxQlFVMHNSVUZCUlN4RFFVRkRMRkZCUVZFc1JVRkJSU3hEUVVGRE8wRkJSWHBETERaRVFVRTJSRHRCUVVNM1JDeFJRVUZSTEVOQlFVTXNaMEpCUVdkQ0xFTkJRVU1zVDBGQlR5eEZRVUZGTEVOQlFVTXNRMEZCWXl4RlFVRkZMRVZCUVVVN1NVRkRjRVFzZFVOQlFYVkRPMGxCUTNaRExFMUJRVTBzU1VGQlNTeEhRVUZITEVOQlFVTXNRMEZCUXl4TlFVRk5MRU5CUVVNN1NVRkRkRUlzU1VGQlNTeExRVUZMTEVOQlFVTXNUMEZCVHl4RFFVRkRMRWxCUVVrc1EwRkJReXhGUVVGRk8xRkJRM1pDTEVsQlFVa3NRMEZCUXl4UFFVRlBMRU5CUVVNc1EwRkJReXhIUVVGSExFVkJRVVVzUlVGQlJUdFpRVU51UWl4UFFVRlBMRU5CUVVNc1IwRkJSeXhEUVVGRExFbEJRVWtzUlVGQlJTeEhRVUZITEVOQlFVTXNUMEZCVHl4RFFVRkRMRU5CUVVNN1VVRkRha01zUTBGQlF5eERRVUZETEVOQlFVTTdTMEZEU2p0VFFVRk5PMUZCUTB3c1QwRkJUeXhEUVVGRExFbEJRVWtzUTBGQlF5eEpRVUZKTEVWQlFVVXNTVUZCU1N4RFFVRkRMRTlCUVU4c1EwRkJReXhEUVVGRE8wdEJRMnhETzBGQlEwZ3NRMEZCUXl4RFFVRkRMRU5CUVVNN1FVRkZTQ3hOUVVGTkxFTkJRVU1zVFVGQlRTeHZRMEZCYjBNc1IwRkJSeXhEUVVGRExHMUNRVUVyUXl4RlFVRkZMRVZCUVVVN1NVRkRkRWNzV1VGQldTeERRVU5XTEhGQ1FVRnhRaXhEUVVGRExHMUNRVUZ0UWl4RFFVRkRMSGxDUVVGNVFpeERRVUZETEVWQlEzQkZMRTlCUVU4c1JVRkRVQ3h0UWtGQmJVSXNRMEZCUXl4UFFVRlBMRU5CUXpWQ0xFTkJRVU03UVVGRFNpeERRVUZETEVOQlFVRWlmUT09IiwiLyogZXNsaW50LWRpc2FibGUgbm8tY29uc29sZSAqL1xuLy8gQ29kZSBiZWxvdyBpcyBub3QgYSBjb250ZW50IHNjcmlwdDogbm8gRmlyZWZveCBBUElzIHNob3VsZCBiZSB1c2VkXG4vLyBBbHNvLCBubyB3ZWJwYWNrL2VzNiBpbXBvcnRzIG1heSBiZSB1c2VkIGluIHRoaXMgZmlsZSBzaW5jZSB0aGUgc2NyaXB0XG4vLyBpcyBleHBvcnRlZCBhcyBhIHBhZ2Ugc2NyaXB0IGFzIGEgc3RyaW5nXG5leHBvcnQgZnVuY3Rpb24gcGFnZVNjcmlwdChnZXRJbnN0cnVtZW50SlMsIGpzSW5zdHJ1bWVudGF0aW9uU2V0dGluZ3MpIHtcbiAgICAvLyBtZXNzYWdlcyB0aGUgaW5qZWN0ZWQgc2NyaXB0XG4gICAgY29uc3Qgc2VuZE1lc3NhZ2VzVG9Mb2dnZXIgPSAoZXZlbnRJZCwgbWVzc2FnZXMpID0+IHtcbiAgICAgICAgZG9jdW1lbnQuZGlzcGF0Y2hFdmVudChuZXcgQ3VzdG9tRXZlbnQoZXZlbnRJZCwge1xuICAgICAgICAgICAgZGV0YWlsOiBtZXNzYWdlcyxcbiAgICAgICAgfSkpO1xuICAgIH07XG4gICAgY29uc3QgZXZlbnRJZCA9IGRvY3VtZW50LmN1cnJlbnRTY3JpcHQuZ2V0QXR0cmlidXRlKFwiZGF0YS1ldmVudC1pZFwiKTtcbiAgICBjb25zdCB0ZXN0aW5nID0gZG9jdW1lbnQuY3VycmVudFNjcmlwdC5nZXRBdHRyaWJ1dGUoXCJkYXRhLXRlc3RpbmdcIik7XG4gICAgY29uc3QgaW5zdHJ1bWVudEpTID0gZ2V0SW5zdHJ1bWVudEpTKGV2ZW50SWQsIHNlbmRNZXNzYWdlc1RvTG9nZ2VyKTtcbiAgICBsZXQgdDA7XG4gICAgaWYgKHRlc3RpbmcgPT09IFwidHJ1ZVwiKSB7XG4gICAgICAgIGNvbnNvbGUubG9nKFwiT3BlbldQTTogQ3VycmVudGx5IHRlc3RpbmdcIik7XG4gICAgICAgIHQwID0gcGVyZm9ybWFuY2Uubm93KCk7XG4gICAgICAgIGNvbnNvbGUubG9nKFwiQmVnaW4gbG9hZGluZyBKUyBpbnN0cnVtZW50YXRpb24uXCIpO1xuICAgIH1cbiAgICBpbnN0cnVtZW50SlMoanNJbnN0cnVtZW50YXRpb25TZXR0aW5ncyk7XG4gICAgaWYgKHRlc3RpbmcgPT09IFwidHJ1ZVwiKSB7XG4gICAgICAgIGNvbnN0IHQxID0gcGVyZm9ybWFuY2Uubm93KCk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBDYWxsIHRvIGluc3RydW1lbnRKUyB0b29rICR7dDEgLSB0MH0gbWlsbGlzZWNvbmRzLmApO1xuICAgICAgICB3aW5kb3cuaW5zdHJ1bWVudEpTID0gaW5zdHJ1bWVudEpTO1xuICAgICAgICBjb25zb2xlLmxvZyhcIk9wZW5XUE06IENvbnRlbnQtc2lkZSBqYXZhc2NyaXB0IGluc3RydW1lbnRhdGlvbiBzdGFydGVkIHdpdGggc3BlYzpcIiwganNJbnN0cnVtZW50YXRpb25TZXR0aW5ncywgbmV3IERhdGUoKS50b0lTT1N0cmluZygpLCBcIihpZiBzcGVjIGlzICc8dW5hdmFpbGFibGU+JyBjaGVjayB3ZWIgY29uc29sZS4pXCIpO1xuICAgIH1cbn1cbjtcbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWFtRjJZWE5qY21sd2RDMXBibk4wY25WdFpXNTBMWEJoWjJVdGMyTnZjR1V1YW5NaUxDSnpiM1Z5WTJWU2IyOTBJam9pSWl3aWMyOTFjbU5sY3lJNld5SXVMaTh1TGk4dUxpOXpjbU12WTI5dWRHVnVkQzlxWVhaaGMyTnlhWEIwTFdsdWMzUnlkVzFsYm5RdGNHRm5aUzF6WTI5d1pTNTBjeUpkTENKdVlXMWxjeUk2VzEwc0ltMWhjSEJwYm1keklqb2lRVUZCUVN3clFrRkJLMEk3UVVGREwwSXNjVVZCUVhGRk8wRkJRM0pGTEhsRlFVRjVSVHRCUVVONlJTd3lRMEZCTWtNN1FVRkZNME1zVFVGQlRTeFZRVUZWTEZWQlFWVXNRMEZCUlN4bFFVRmxMRVZCUVVVc2VVSkJRWGxDTzBsQlEzQkZMQ3RDUVVFclFqdEpRVU12UWl4TlFVRk5MRzlDUVVGdlFpeEhRVUZITEVOQlFVTXNUMEZCVHl4RlFVRkZMRkZCUVZFc1JVRkJSU3hGUVVGRk8xRkJRMnBFTEZGQlFWRXNRMEZCUXl4aFFVRmhMRU5CUTNCQ0xFbEJRVWtzVjBGQlZ5eERRVUZETEU5QlFVOHNSVUZCUlR0WlFVTjJRaXhOUVVGTkxFVkJRVVVzVVVGQlVUdFRRVU5xUWl4RFFVRkRMRU5CUTBnc1EwRkJRenRKUVVOS0xFTkJRVU1zUTBGQlF6dEpRVVZHTEUxQlFVMHNUMEZCVHl4SFFVRkhMRkZCUVZFc1EwRkJReXhoUVVGaExFTkJRVU1zV1VGQldTeERRVUZETEdWQlFXVXNRMEZCUXl4RFFVRkRPMGxCUTNKRkxFMUJRVTBzVDBGQlR5eEhRVUZITEZGQlFWRXNRMEZCUXl4aFFVRmhMRU5CUVVNc1dVRkJXU3hEUVVGRExHTkJRV01zUTBGQlF5eERRVUZETzBsQlEzQkZMRTFCUVUwc1dVRkJXU3hIUVVGSExHVkJRV1VzUTBGQlF5eFBRVUZQTEVWQlFVVXNiMEpCUVc5Q0xFTkJRVU1zUTBGQlF6dEpRVU53UlN4SlFVRkpMRVZCUVZVc1EwRkJRenRKUVVObUxFbEJRVWtzVDBGQlR5eExRVUZMTEUxQlFVMHNSVUZCUlR0UlFVTjBRaXhQUVVGUExFTkJRVU1zUjBGQlJ5eERRVUZETERSQ1FVRTBRaXhEUVVGRExFTkJRVU03VVVGRE1VTXNSVUZCUlN4SFFVRkhMRmRCUVZjc1EwRkJReXhIUVVGSExFVkJRVVVzUTBGQlF6dFJRVU4yUWl4UFFVRlBMRU5CUVVNc1IwRkJSeXhEUVVGRExHMURRVUZ0UXl4RFFVRkRMRU5CUVVNN1MwRkRiRVE3U1VGRFJDeFpRVUZaTEVOQlFVTXNlVUpCUVhsQ0xFTkJRVU1zUTBGQlF6dEpRVU40UXl4SlFVRkpMRTlCUVU4c1MwRkJTeXhOUVVGTkxFVkJRVVU3VVVGRGRFSXNUVUZCVFN4RlFVRkZMRWRCUVVjc1YwRkJWeXhEUVVGRExFZEJRVWNzUlVGQlJTeERRVUZETzFGQlF6ZENMRTlCUVU4c1EwRkJReXhIUVVGSExFTkJRVU1zTmtKQlFUWkNMRVZCUVVVc1IwRkJSeXhGUVVGRkxHZENRVUZuUWl4RFFVRkRMRU5CUVVNN1VVRkRha1VzVFVGQll5eERRVUZETEZsQlFWa3NSMEZCUnl4WlFVRlpMRU5CUVVNN1VVRkROVU1zVDBGQlR5eERRVUZETEVkQlFVY3NRMEZEVkN4eFJVRkJjVVVzUlVGRGNrVXNlVUpCUVhsQ0xFVkJRM3BDTEVsQlFVa3NTVUZCU1N4RlFVRkZMRU5CUVVNc1YwRkJWeXhGUVVGRkxFVkJRM2hDTEdsRVFVRnBSQ3hEUVVOc1JDeERRVUZETzB0QlEwZzdRVUZEU0N4RFFVRkRPMEZCUVVFc1EwRkJReUo5IiwiZXhwb3J0ICogZnJvbSBcIi4vYmFja2dyb3VuZC9jb29raWUtaW5zdHJ1bWVudFwiO1xuZXhwb3J0ICogZnJvbSBcIi4vYmFja2dyb3VuZC9kbnMtaW5zdHJ1bWVudFwiO1xuZXhwb3J0ICogZnJvbSBcIi4vYmFja2dyb3VuZC9odHRwLWluc3RydW1lbnRcIjtcbmV4cG9ydCAqIGZyb20gXCIuL2JhY2tncm91bmQvamF2YXNjcmlwdC1pbnN0cnVtZW50XCI7XG5leHBvcnQgKiBmcm9tIFwiLi9iYWNrZ3JvdW5kL25hdmlnYXRpb24taW5zdHJ1bWVudFwiO1xuZXhwb3J0ICogZnJvbSBcIi4vY29udGVudC9qYXZhc2NyaXB0LWluc3RydW1lbnQtY29udGVudC1zY29wZVwiO1xuZXhwb3J0ICogZnJvbSBcIi4vbGliL2h0dHAtcG9zdC1wYXJzZXJcIjtcbmV4cG9ydCAqIGZyb20gXCIuL2xpYi9zdHJpbmctdXRpbHNcIjtcbmV4cG9ydCAqIGZyb20gXCIuL3NjaGVtYVwiO1xuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pYVc1a1pYZ3Vhbk1pTENKemIzVnlZMlZTYjI5MElqb2lJaXdpYzI5MWNtTmxjeUk2V3lJdUxpOHVMaTl6Y21NdmFXNWtaWGd1ZEhNaVhTd2libUZ0WlhNaU9sdGRMQ0p0WVhCd2FXNW5jeUk2SWtGQlFVRXNZMEZCWXl4blEwRkJaME1zUTBGQlF6dEJRVU12UXl4alFVRmpMRFpDUVVFMlFpeERRVUZETzBGQlF6VkRMR05CUVdNc09FSkJRVGhDTEVOQlFVTTdRVUZETjBNc1kwRkJZeXh2UTBGQmIwTXNRMEZCUXp0QlFVTnVSQ3hqUVVGakxHOURRVUZ2UXl4RFFVRkRPMEZCUTI1RUxHTkJRV01zSzBOQlFTdERMRU5CUVVNN1FVRkRPVVFzWTBGQll5eDNRa0ZCZDBJc1EwRkJRenRCUVVOMlF5eGpRVUZqTEc5Q1FVRnZRaXhEUVVGRE8wRkJRMjVETEdOQlFXTXNWVUZCVlN4RFFVRkRJbjA9IiwiLyoqXG4gKiBUaGlzIGVuYWJsZXMgdXMgdG8ga2VlcCBpbmZvcm1hdGlvbiBhYm91dCB0aGUgb3JpZ2luYWwgb3JkZXJcbiAqIGluIHdoaWNoIGV2ZW50cyBhcnJpdmVkIHRvIG91ciBldmVudCBsaXN0ZW5lcnMuXG4gKi9cbmxldCBldmVudE9yZGluYWwgPSAwO1xuZXhwb3J0IGNvbnN0IGluY3JlbWVudGVkRXZlbnRPcmRpbmFsID0gKCkgPT4ge1xuICAgIHJldHVybiBldmVudE9yZGluYWwrKztcbn07XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2laWGgwWlc1emFXOXVMWE5sYzNOcGIyNHRaWFpsYm5RdGIzSmthVzVoYkM1cWN5SXNJbk52ZFhKalpWSnZiM1FpT2lJaUxDSnpiM1Z5WTJWeklqcGJJaTR1THk0dUx5NHVMM055WXk5c2FXSXZaWGgwWlc1emFXOXVMWE5sYzNOcGIyNHRaWFpsYm5RdGIzSmthVzVoYkM1MGN5SmRMQ0p1WVcxbGN5STZXMTBzSW0xaGNIQnBibWR6SWpvaVFVRkJRVHM3TzBkQlIwYzdRVUZEU0N4SlFVRkpMRmxCUVZrc1IwRkJSeXhEUVVGRExFTkJRVU03UVVGRmNrSXNUVUZCVFN4RFFVRkRMRTFCUVUwc2RVSkJRWFZDTEVkQlFVY3NSMEZCUnl4RlFVRkZPMGxCUXpGRExFOUJRVThzV1VGQldTeEZRVUZGTEVOQlFVTTdRVUZEZUVJc1EwRkJReXhEUVVGREluMD0iLCJpbXBvcnQgeyBtYWtlVVVJRCB9IGZyb20gXCIuL3V1aWRcIjtcbi8qKlxuICogVGhpcyBlbmFibGVzIHVzIHRvIGFjY2VzcyBhIHVuaXF1ZSByZWZlcmVuY2UgdG8gdGhpcyBicm93c2VyXG4gKiBzZXNzaW9uIC0gcmVnZW5lcmF0ZWQgYW55IHRpbWUgdGhlIGJhY2tncm91bmQgcHJvY2VzcyBnZXRzXG4gKiByZXN0YXJ0ZWQgKHdoaWNoIHNob3VsZCBvbmx5IGJlIG9uIGJyb3dzZXIgcmVzdGFydHMpXG4gKi9cbmV4cG9ydCBjb25zdCBleHRlbnNpb25TZXNzaW9uVXVpZCA9IG1ha2VVVUlEKCk7XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2laWGgwWlc1emFXOXVMWE5sYzNOcGIyNHRkWFZwWkM1cWN5SXNJbk52ZFhKalpWSnZiM1FpT2lJaUxDSnpiM1Z5WTJWeklqcGJJaTR1THk0dUx5NHVMM055WXk5c2FXSXZaWGgwWlc1emFXOXVMWE5sYzNOcGIyNHRkWFZwWkM1MGN5SmRMQ0p1WVcxbGN5STZXMTBzSW0xaGNIQnBibWR6SWpvaVFVRkJRU3hQUVVGUExFVkJRVVVzVVVGQlVTeEZRVUZGTEUxQlFVMHNVVUZCVVN4RFFVRkRPMEZCUld4RE96czdPMGRCU1VjN1FVRkRTQ3hOUVVGTkxFTkJRVU1zVFVGQlRTeHZRa0ZCYjBJc1IwRkJSeXhSUVVGUkxFVkJRVVVzUTBGQlF5SjkiLCJpbXBvcnQgeyBlc2NhcGVTdHJpbmcsIFVpbnQ4VG9CYXNlNjQgfSBmcm9tIFwiLi9zdHJpbmctdXRpbHNcIjtcbmV4cG9ydCBjbGFzcyBIdHRwUG9zdFBhcnNlciB7XG4gICAgb25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzO1xuICAgIGRhdGFSZWNlaXZlcjtcbiAgICBjb25zdHJ1Y3RvcihvbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHMsIGRhdGFSZWNlaXZlcikge1xuICAgICAgICB0aGlzLm9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscyA9IG9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscztcbiAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIgPSBkYXRhUmVjZWl2ZXI7XG4gICAgfVxuICAgIHBhcnNlUG9zdFJlcXVlc3QoKSB7XG4gICAgICAgIGNvbnN0IHJlcXVlc3RCb2R5ID0gdGhpcy5vbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHMucmVxdWVzdEJvZHk7XG4gICAgICAgIGlmIChyZXF1ZXN0Qm9keS5lcnJvcikge1xuICAgICAgICAgICAgdGhpcy5kYXRhUmVjZWl2ZXIubG9nRXJyb3IoXCJFeGNlcHRpb246IFVwc3RyZWFtIGZhaWxlZCB0byBwYXJzZSBQT1NUOiBcIiArIHJlcXVlc3RCb2R5LmVycm9yKTtcbiAgICAgICAgfVxuICAgICAgICBpZiAocmVxdWVzdEJvZHkuZm9ybURhdGEpIHtcbiAgICAgICAgICAgIHJldHVybiB7XG4gICAgICAgICAgICAgICAgcG9zdF9ib2R5OiBlc2NhcGVTdHJpbmcoSlNPTi5zdHJpbmdpZnkocmVxdWVzdEJvZHkuZm9ybURhdGEpKSxcbiAgICAgICAgICAgIH07XG4gICAgICAgIH1cbiAgICAgICAgaWYgKHJlcXVlc3RCb2R5LnJhdykge1xuICAgICAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICAgICAgICBwb3N0X2JvZHlfcmF3OiBKU09OLnN0cmluZ2lmeShyZXF1ZXN0Qm9keS5yYXcubWFwKCh4KSA9PiBbXG4gICAgICAgICAgICAgICAgICAgIHguZmlsZSxcbiAgICAgICAgICAgICAgICAgICAgVWludDhUb0Jhc2U2NChuZXcgVWludDhBcnJheSh4LmJ5dGVzKSksXG4gICAgICAgICAgICAgICAgXSkpLFxuICAgICAgICAgICAgfTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4ge307XG4gICAgfVxufVxuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pYUhSMGNDMXdiM04wTFhCaGNuTmxjaTVxY3lJc0luTnZkWEpqWlZKdmIzUWlPaUlpTENKemIzVnlZMlZ6SWpwYklpNHVMeTR1THk0dUwzTnlZeTlzYVdJdmFIUjBjQzF3YjNOMExYQmhjbk5sY2k1MGN5SmRMQ0p1WVcxbGN5STZXMTBzSW0xaGNIQnBibWR6SWpvaVFVRkRRU3hQUVVGUExFVkJRVVVzV1VGQldTeEZRVUZGTEdGQlFXRXNSVUZCUlN4TlFVRk5MR2RDUVVGblFpeERRVUZETzBGQlVUZEVMRTFCUVUwc1QwRkJUeXhqUVVGak8wbEJRMUlzTWtKQlFUSkNMRU5CUVhkRE8wbEJRMjVGTEZsQlFWa3NRMEZCUXp0SlFVVTVRaXhaUVVORkxESkNRVUZyUlN4RlFVTnNSU3haUVVGWk8xRkJSVm9zU1VGQlNTeERRVUZETERKQ1FVRXlRaXhIUVVGSExESkNRVUV5UWl4RFFVRkRPMUZCUXk5RUxFbEJRVWtzUTBGQlF5eFpRVUZaTEVkQlFVY3NXVUZCV1N4RFFVRkRPMGxCUTI1RExFTkJRVU03U1VGRlRTeG5Ra0ZCWjBJN1VVRkRja0lzVFVGQlRTeFhRVUZYTEVkQlFVY3NTVUZCU1N4RFFVRkRMREpDUVVFeVFpeERRVUZETEZkQlFWY3NRMEZCUXp0UlFVTnFSU3hKUVVGSkxGZEJRVmNzUTBGQlF5eExRVUZMTEVWQlFVVTdXVUZEY2tJc1NVRkJTU3hEUVVGRExGbEJRVmtzUTBGQlF5eFJRVUZSTEVOQlEzaENMRFJEUVVFMFF5eEhRVUZITEZkQlFWY3NRMEZCUXl4TFFVRkxMRU5CUTJwRkxFTkJRVU03VTBGRFNEdFJRVU5FTEVsQlFVa3NWMEZCVnl4RFFVRkRMRkZCUVZFc1JVRkJSVHRaUVVONFFpeFBRVUZQTzJkQ1FVTk1MRk5CUVZNc1JVRkJSU3haUVVGWkxFTkJRVU1zU1VGQlNTeERRVUZETEZOQlFWTXNRMEZCUXl4WFFVRlhMRU5CUVVNc1VVRkJVU3hEUVVGRExFTkJRVU03WVVGRE9VUXNRMEZCUXp0VFFVTklPMUZCUTBRc1NVRkJTU3hYUVVGWExFTkJRVU1zUjBGQlJ5eEZRVUZGTzFsQlEyNUNMRTlCUVU4N1owSkJRMHdzWVVGQllTeEZRVUZGTEVsQlFVa3NRMEZCUXl4VFFVRlRMRU5CUXpOQ0xGZEJRVmNzUTBGQlF5eEhRVUZITEVOQlFVTXNSMEZCUnl4RFFVRkRMRU5CUVVNc1EwRkJReXhGUVVGRkxFVkJRVVVzUTBGQlF6dHZRa0ZEZWtJc1EwRkJReXhEUVVGRExFbEJRVWs3YjBKQlEwNHNZVUZCWVN4RFFVRkRMRWxCUVVrc1ZVRkJWU3hEUVVGRExFTkJRVU1zUTBGQlF5eExRVUZMTEVOQlFVTXNRMEZCUXp0cFFrRkRka01zUTBGQlF5eERRVU5JTzJGQlEwWXNRMEZCUXp0VFFVTklPMUZCUTBRc1QwRkJUeXhGUVVGRkxFTkJRVU03U1VGRFdpeERRVUZETzBOQlEwWWlmUT09IiwiLy8gSW50cnVtZW50YXRpb24gaW5qZWN0aW9uIGNvZGUgaXMgYmFzZWQgb24gcHJpdmFjeWJhZGdlcmZpcmVmb3hcbi8vIGh0dHBzOi8vZ2l0aHViLmNvbS9FRkZvcmcvcHJpdmFjeWJhZGdlcmZpcmVmb3gvYmxvYi9tYXN0ZXIvZGF0YS9maW5nZXJwcmludGluZy5qc1xuZXhwb3J0IGZ1bmN0aW9uIGdldEluc3RydW1lbnRKUyhldmVudElkLCBzZW5kTWVzc2FnZXNUb0xvZ2dlcikge1xuICAgIC8qXG4gICAgICogSW5zdHJ1bWVudGF0aW9uIGhlbHBlcnNcbiAgICAgKiAoSW5saW5lZCBpbiBvcmRlciBmb3IganNJbnN0cnVtZW50cyB0byBiZSBlYXNpbHkgZXhwb3J0YWJsZSBhcyBhIHN0cmluZylcbiAgICAgKi9cbiAgICAvLyBmdW5jdGlvbiBnZXRBdHRyaWJ1dGVzIChhdHRyaWJ1dGVzKSB7XG4gICAgLy8gICAgIHJldHVybiBBcnJheS5mcm9tKGF0dHJpYnV0ZXMpXG4gICAgLy8gICAgICAgLm1hcChhID0+IFthLm5hbWUsIGEudmFsdWVdKVxuICAgIC8vICAgICAgIC5yZWR1Y2UoKGFjYywgYXR0cikgPT4ge1xuICAgIC8vICAgICAgICAgYWNjW2F0dHJbMF1dID0gYXR0clsxXVxuICAgIC8vICAgICAgICAgcmV0dXJuIGFjY1xuICAgIC8vICAgICAgIH0sIHt9KVxuICAgIC8vICAgfVxuICAgIC8vIENvdW50ZXIgdG8gY2FwICMgb2YgY2FsbHMgbG9nZ2VkIGZvciBlYWNoIHNjcmlwdC9hcGkgY29tYmluYXRpb25cbiAgICBjb25zdCBtYXhMb2dDb3VudCA9IDUwMDtcbiAgICAvLyBsb2dDb3VudGVyXG4gICAgY29uc3QgbG9nQ291bnRlciA9IG5ldyBPYmplY3QoKTtcbiAgICAvLyBQcmV2ZW50IGxvZ2dpbmcgb2YgZ2V0cyBhcmlzaW5nIGZyb20gbG9nZ2luZ1xuICAgIGxldCBpbkxvZyA9IGZhbHNlO1xuICAgIC8vIFRvIGtlZXAgdHJhY2sgb2YgdGhlIG9yaWdpbmFsIG9yZGVyIG9mIGV2ZW50c1xuICAgIGxldCBvcmRpbmFsID0gMDtcbiAgICAvLyBPcHRpb25zIGZvciBKU09wZXJhdGlvblxuICAgIGNvbnN0IEpTT3BlcmF0aW9uID0ge1xuICAgICAgICBjYWxsOiBcImNhbGxcIixcbiAgICAgICAgZ2V0OiBcImdldFwiLFxuICAgICAgICBnZXRfZmFpbGVkOiBcImdldChmYWlsZWQpXCIsXG4gICAgICAgIGdldF9mdW5jdGlvbjogXCJnZXQoZnVuY3Rpb24pXCIsXG4gICAgICAgIHNldDogXCJzZXRcIixcbiAgICAgICAgc2V0X2ZhaWxlZDogXCJzZXQoZmFpbGVkKVwiLFxuICAgICAgICBzZXRfcHJldmVudGVkOiBcInNldChwcmV2ZW50ZWQpXCIsXG4gICAgfTtcbiAgICAvLyBSb3VnaCBpbXBsZW1lbnRhdGlvbnMgb2YgT2JqZWN0LmdldFByb3BlcnR5RGVzY3JpcHRvciBhbmQgT2JqZWN0LmdldFByb3BlcnR5TmFtZXNcbiAgICAvLyBTZWUgaHR0cDovL3dpa2kuZWNtYXNjcmlwdC5vcmcvZG9rdS5waHA/aWQ9aGFybW9ueTpleHRlbmRlZF9vYmplY3RfYXBpXG4gICAgT2JqZWN0LmdldFByb3BlcnR5RGVzY3JpcHRvciA9IGZ1bmN0aW9uIChzdWJqZWN0LCBuYW1lKSB7XG4gICAgICAgIGlmIChzdWJqZWN0ID09PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIHRocm93IG5ldyBFcnJvcihcIkNhbid0IGdldCBwcm9wZXJ0eSBkZXNjcmlwdG9yIGZvciB1bmRlZmluZWRcIik7XG4gICAgICAgIH1cbiAgICAgICAgbGV0IHBkID0gT2JqZWN0LmdldE93blByb3BlcnR5RGVzY3JpcHRvcihzdWJqZWN0LCBuYW1lKTtcbiAgICAgICAgbGV0IHByb3RvID0gT2JqZWN0LmdldFByb3RvdHlwZU9mKHN1YmplY3QpO1xuICAgICAgICB3aGlsZSAocGQgPT09IHVuZGVmaW5lZCAmJiBwcm90byAhPT0gbnVsbCkge1xuICAgICAgICAgICAgcGQgPSBPYmplY3QuZ2V0T3duUHJvcGVydHlEZXNjcmlwdG9yKHByb3RvLCBuYW1lKTtcbiAgICAgICAgICAgIHByb3RvID0gT2JqZWN0LmdldFByb3RvdHlwZU9mKHByb3RvKTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gcGQ7XG4gICAgfTtcbiAgICBPYmplY3QuZ2V0UHJvcGVydHlOYW1lcyA9IGZ1bmN0aW9uIChzdWJqZWN0KSB7XG4gICAgICAgIGlmIChzdWJqZWN0ID09PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIHRocm93IG5ldyBFcnJvcihcIkNhbid0IGdldCBwcm9wZXJ0eSBuYW1lcyBmb3IgdW5kZWZpbmVkXCIpO1xuICAgICAgICB9XG4gICAgICAgIGxldCBwcm9wcyA9IE9iamVjdC5nZXRPd25Qcm9wZXJ0eU5hbWVzKHN1YmplY3QpO1xuICAgICAgICBsZXQgcHJvdG8gPSBPYmplY3QuZ2V0UHJvdG90eXBlT2Yoc3ViamVjdCk7XG4gICAgICAgIHdoaWxlIChwcm90byAhPT0gbnVsbCkge1xuICAgICAgICAgICAgcHJvcHMgPSBwcm9wcy5jb25jYXQoT2JqZWN0LmdldE93blByb3BlcnR5TmFtZXMocHJvdG8pKTtcbiAgICAgICAgICAgIHByb3RvID0gT2JqZWN0LmdldFByb3RvdHlwZU9mKHByb3RvKTtcbiAgICAgICAgfVxuICAgICAgICAvLyBGSVhNRTogcmVtb3ZlIGR1cGxpY2F0ZSBwcm9wZXJ0eSBuYW1lcyBmcm9tIHByb3BzXG4gICAgICAgIHJldHVybiBwcm9wcztcbiAgICB9O1xuICAgIC8vIGRlYm91bmNlIC0gZnJvbSBVbmRlcnNjb3JlIHYxLjYuMFxuICAgIGZ1bmN0aW9uIGRlYm91bmNlKGZ1bmMsIHdhaXQsIGltbWVkaWF0ZSA9IGZhbHNlKSB7XG4gICAgICAgIGxldCB0aW1lb3V0O1xuICAgICAgICBsZXQgYXJncztcbiAgICAgICAgbGV0IGNvbnRleHQ7XG4gICAgICAgIGxldCB0aW1lc3RhbXA7XG4gICAgICAgIGxldCByZXN1bHQ7XG4gICAgICAgIGNvbnN0IGxhdGVyID0gZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgY29uc3QgbGFzdCA9IERhdGUubm93KCkgLSB0aW1lc3RhbXA7XG4gICAgICAgICAgICBpZiAobGFzdCA8IHdhaXQpIHtcbiAgICAgICAgICAgICAgICB0aW1lb3V0ID0gc2V0VGltZW91dChsYXRlciwgd2FpdCAtIGxhc3QpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgdGltZW91dCA9IG51bGw7XG4gICAgICAgICAgICAgICAgaWYgKCFpbW1lZGlhdGUpIHtcbiAgICAgICAgICAgICAgICAgICAgcmVzdWx0ID0gZnVuYy5hcHBseShjb250ZXh0LCBhcmdzKTtcbiAgICAgICAgICAgICAgICAgICAgY29udGV4dCA9IGFyZ3MgPSBudWxsO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgfTtcbiAgICAgICAgcmV0dXJuIGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgIGNvbnRleHQgPSB0aGlzO1xuICAgICAgICAgICAgYXJncyA9IGFyZ3VtZW50cztcbiAgICAgICAgICAgIHRpbWVzdGFtcCA9IERhdGUubm93KCk7XG4gICAgICAgICAgICBjb25zdCBjYWxsTm93ID0gaW1tZWRpYXRlICYmICF0aW1lb3V0O1xuICAgICAgICAgICAgaWYgKCF0aW1lb3V0KSB7XG4gICAgICAgICAgICAgICAgdGltZW91dCA9IHNldFRpbWVvdXQobGF0ZXIsIHdhaXQpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgaWYgKGNhbGxOb3cpIHtcbiAgICAgICAgICAgICAgICByZXN1bHQgPSBmdW5jLmFwcGx5KGNvbnRleHQsIGFyZ3MpO1xuICAgICAgICAgICAgICAgIGNvbnRleHQgPSBhcmdzID0gbnVsbDtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIHJldHVybiByZXN1bHQ7XG4gICAgICAgIH07XG4gICAgfVxuICAgIC8vIFJlY3Vyc2l2ZWx5IGdlbmVyYXRlcyBhIHBhdGggZm9yIGFuIGVsZW1lbnRcbiAgICBmdW5jdGlvbiBnZXRQYXRoVG9Eb21FbGVtZW50KGVsZW1lbnQsIHZpc2liaWxpdHlBdHRyID0gZmFsc2UpIHtcbiAgICAgICAgaWYgKGVsZW1lbnQgPT09IGRvY3VtZW50LmJvZHkpIHtcbiAgICAgICAgICAgIHJldHVybiBlbGVtZW50LnRhZ05hbWU7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsZW1lbnQucGFyZW50Tm9kZSA9PT0gbnVsbCkge1xuICAgICAgICAgICAgcmV0dXJuIFwiTlVMTC9cIiArIGVsZW1lbnQudGFnTmFtZTtcbiAgICAgICAgfVxuICAgICAgICBsZXQgc2libGluZ0luZGV4ID0gMTtcbiAgICAgICAgY29uc3Qgc2libGluZ3MgPSBlbGVtZW50LnBhcmVudE5vZGUuY2hpbGROb2RlcztcbiAgICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBzaWJsaW5ncy5sZW5ndGg7IGkrKykge1xuICAgICAgICAgICAgY29uc3Qgc2libGluZyA9IHNpYmxpbmdzW2ldO1xuICAgICAgICAgICAgaWYgKHNpYmxpbmcgPT09IGVsZW1lbnQpIHtcbiAgICAgICAgICAgICAgICBsZXQgcGF0aCA9IGdldFBhdGhUb0RvbUVsZW1lbnQoZWxlbWVudC5wYXJlbnROb2RlLCB2aXNpYmlsaXR5QXR0cik7XG4gICAgICAgICAgICAgICAgcGF0aCArPSBcIi9cIiArIGVsZW1lbnQudGFnTmFtZSArIFwiW1wiICsgc2libGluZ0luZGV4O1xuICAgICAgICAgICAgICAgIHBhdGggKz0gXCIsXCIgKyBlbGVtZW50LmlkO1xuICAgICAgICAgICAgICAgIHBhdGggKz0gXCIsXCIgKyBlbGVtZW50LmNsYXNzTmFtZTtcbiAgICAgICAgICAgICAgICBpZiAodmlzaWJpbGl0eUF0dHIpIHtcbiAgICAgICAgICAgICAgICAgICAgcGF0aCArPSBcIixcIiArIGVsZW1lbnQuaGlkZGVuO1xuICAgICAgICAgICAgICAgICAgICBwYXRoICs9IFwiLFwiICsgZWxlbWVudC5zdHlsZS5kaXNwbGF5O1xuICAgICAgICAgICAgICAgICAgICBwYXRoICs9IFwiLFwiICsgZWxlbWVudC5zdHlsZS52aXNpYmlsaXR5O1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBpZiAoZWxlbWVudC50YWdOYW1lID09PSBcIkFcIikge1xuICAgICAgICAgICAgICAgICAgICBwYXRoICs9IFwiLFwiICsgZWxlbWVudC5ocmVmO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBwYXRoICs9IFwiXVwiO1xuICAgICAgICAgICAgICAgIHJldHVybiBwYXRoO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgaWYgKHNpYmxpbmcubm9kZVR5cGUgPT09IDEgJiYgc2libGluZy50YWdOYW1lID09PSBlbGVtZW50LnRhZ05hbWUpIHtcbiAgICAgICAgICAgICAgICBzaWJsaW5nSW5kZXgrKztcbiAgICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgIH1cbiAgICAvLyBIZWxwZXIgZm9yIEpTT05pZnlpbmcgb2JqZWN0c1xuICAgIGZ1bmN0aW9uIHNlcmlhbGl6ZU9iamVjdChvYmplY3QsIHN0cmluZ2lmeUZ1bmN0aW9ucyA9IGZhbHNlKSB7XG4gICAgICAgIC8vIEhhbmRsZSBwZXJtaXNzaW9ucyBlcnJvcnNcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgIGlmIChvYmplY3QgPT09IG51bGwpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gXCJudWxsXCI7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBpZiAodHlwZW9mIG9iamVjdCA9PT0gXCJmdW5jdGlvblwiKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIHN0cmluZ2lmeUZ1bmN0aW9ucyA/IG9iamVjdC50b1N0cmluZygpIDogXCJGVU5DVElPTlwiO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgaWYgKHR5cGVvZiBvYmplY3QgIT09IFwib2JqZWN0XCIpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gb2JqZWN0O1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgY29uc3Qgc2Vlbk9iamVjdHMgPSBbXTtcbiAgICAgICAgICAgIHJldHVybiBKU09OLnN0cmluZ2lmeShvYmplY3QsIGZ1bmN0aW9uIChrZXksIHZhbHVlKSB7XG4gICAgICAgICAgICAgICAgaWYgKHZhbHVlID09PSBudWxsKSB7XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBcIm51bGxcIjtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gXCJmdW5jdGlvblwiKSB7XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBzdHJpbmdpZnlGdW5jdGlvbnMgPyB2YWx1ZS50b1N0cmluZygpIDogXCJGVU5DVElPTlwiO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBpZiAodHlwZW9mIHZhbHVlID09PSBcIm9iamVjdFwiKSB7XG4gICAgICAgICAgICAgICAgICAgIC8vIFJlbW92ZSB3cmFwcGluZyBvbiBjb250ZW50IG9iamVjdHNcbiAgICAgICAgICAgICAgICAgICAgaWYgKFwid3JhcHBlZEpTT2JqZWN0XCIgaW4gdmFsdWUpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHZhbHVlID0gdmFsdWUud3JhcHBlZEpTT2JqZWN0O1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgIC8vIFNlcmlhbGl6ZSBET00gZWxlbWVudHNcbiAgICAgICAgICAgICAgICAgICAgaWYgKHZhbHVlIGluc3RhbmNlb2YgSFRNTEVsZW1lbnQpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBnZXRQYXRoVG9Eb21FbGVtZW50KHZhbHVlKTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAvLyBQcmV2ZW50IHNlcmlhbGl6YXRpb24gY3ljbGVzXG4gICAgICAgICAgICAgICAgICAgIGlmIChrZXkgPT09IFwiXCIgfHwgc2Vlbk9iamVjdHMuaW5kZXhPZih2YWx1ZSkgPCAwKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBzZWVuT2JqZWN0cy5wdXNoKHZhbHVlKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiB2YWx1ZTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiB0eXBlb2YgdmFsdWU7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgcmV0dXJuIHZhbHVlO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgICAgICBjb25zb2xlLmxvZyhcIk9wZW5XUE06IFNFUklBTElaQVRJT04gRVJST1I6IFwiICsgZXJyb3IpO1xuICAgICAgICAgICAgcmV0dXJuIFwiU0VSSUFMSVpBVElPTiBFUlJPUjogXCIgKyBlcnJvcjtcbiAgICAgICAgfVxuICAgIH1cbiAgICBmdW5jdGlvbiB1cGRhdGVDb3VudGVyQW5kQ2hlY2tJZk92ZXIoc2NyaXB0VXJsLCBzeW1ib2wpIHtcbiAgICAgICAgY29uc3Qga2V5ID0gc2NyaXB0VXJsICsgXCJ8XCIgKyBzeW1ib2w7XG4gICAgICAgIGlmIChrZXkgaW4gbG9nQ291bnRlciAmJiBsb2dDb3VudGVyW2tleV0gPj0gbWF4TG9nQ291bnQpIHtcbiAgICAgICAgICAgIHJldHVybiB0cnVlO1xuICAgICAgICB9XG4gICAgICAgIGVsc2UgaWYgKCEoa2V5IGluIGxvZ0NvdW50ZXIpKSB7XG4gICAgICAgICAgICBsb2dDb3VudGVyW2tleV0gPSAxO1xuICAgICAgICB9XG4gICAgICAgIGVsc2Uge1xuICAgICAgICAgICAgbG9nQ291bnRlcltrZXldICs9IDE7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgICAvLyBGb3IgZ2V0cywgc2V0cywgZXRjLiBvbiBhIHNpbmdsZSB2YWx1ZVxuICAgIGZ1bmN0aW9uIGxvZ1ZhbHVlKGluc3RydW1lbnRlZFZhcmlhYmxlTmFtZSwgdmFsdWUsIGF0dHJpYnV0ZXMsIG9wZXJhdGlvbiwgLy8gZnJvbSBKU09wZXJhdGlvbiBvYmplY3QgcGxlYXNlXG4gICAgY2FsbENvbnRleHQsIGxvZ1NldHRpbmdzKSB7XG4gICAgICAgIGlmIChpbkxvZykge1xuICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG4gICAgICAgIGluTG9nID0gdHJ1ZTtcbiAgICAgICAgY29uc3Qgb3ZlckxpbWl0ID0gdXBkYXRlQ291bnRlckFuZENoZWNrSWZPdmVyKGNhbGxDb250ZXh0LnNjcmlwdFVybCwgaW5zdHJ1bWVudGVkVmFyaWFibGVOYW1lKTtcbiAgICAgICAgaWYgKG92ZXJMaW1pdCkge1xuICAgICAgICAgICAgaW5Mb2cgPSBmYWxzZTtcbiAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgfVxuICAgICAgICAvL2F0dHJpYnV0ZXMgaXMgYSBOYW1lZE5vZGVNYXAuIERvaW5nIGEgY29udmVyc2lvbiwgdGFraW5nIG9ubHkgbmFtZXMgYW5kIHZhbHVlcy5cbiAgICAgICAgLy9Eb2luZyB0aGlzIG9ubHkgZm9yIHNyYyBjYWxscyBmb3Igbm93LlxuICAgICAgICBpZiAoaW5zdHJ1bWVudGVkVmFyaWFibGVOYW1lLmluY2x1ZGVzKCdFbGVtZW50LnNyYycpKSB7XG4gICAgICAgICAgICB2YXIgY29tcGxldGVVcmwgPSBuZXcgVVJMKHZhbHVlLCB3aW5kb3cubG9jYXRpb24uaHJlZik7XG4gICAgICAgICAgICB2YWx1ZSA9IGNvbXBsZXRlVXJsLmhyZWY7XG4gICAgICAgICAgICB2YXIgbmV3T2JqZWN0ID0gT2JqZWN0LmFzc2lnbih7fSwgQXJyYXkuZnJvbShhdHRyaWJ1dGVzLCAoeyBuYW1lLCB2YWx1ZSB9KSA9PiAoeyBbbmFtZV06IHZhbHVlIH0pKSk7XG4gICAgICAgICAgICBhdHRyaWJ1dGVzID0gbmV3T2JqZWN0O1xuICAgICAgICB9XG4gICAgICAgIGNvbnN0IG1zZyA9IHtcbiAgICAgICAgICAgIG9wZXJhdGlvbixcbiAgICAgICAgICAgIHN5bWJvbDogaW5zdHJ1bWVudGVkVmFyaWFibGVOYW1lLFxuICAgICAgICAgICAgdmFsdWU6IHNlcmlhbGl6ZU9iamVjdCh2YWx1ZSwgbG9nU2V0dGluZ3MubG9nRnVuY3Rpb25zQXNTdHJpbmdzKSxcbiAgICAgICAgICAgIGF0dHJpYnV0ZXM6IHNlcmlhbGl6ZU9iamVjdChhdHRyaWJ1dGVzKSxcbiAgICAgICAgICAgIHNjcmlwdFVybDogY2FsbENvbnRleHQuc2NyaXB0VXJsLFxuICAgICAgICAgICAgc2NyaXB0TGluZTogY2FsbENvbnRleHQuc2NyaXB0TGluZSxcbiAgICAgICAgICAgIHNjcmlwdENvbDogY2FsbENvbnRleHQuc2NyaXB0Q29sLFxuICAgICAgICAgICAgZnVuY05hbWU6IGNhbGxDb250ZXh0LmZ1bmNOYW1lLFxuICAgICAgICAgICAgc2NyaXB0TG9jRXZhbDogY2FsbENvbnRleHQuc2NyaXB0TG9jRXZhbCxcbiAgICAgICAgICAgIGNhbGxTdGFjazogY2FsbENvbnRleHQuY2FsbFN0YWNrLFxuICAgICAgICAgICAgb3JkaW5hbDogb3JkaW5hbCsrLFxuICAgICAgICB9O1xuICAgICAgICB0cnkge1xuICAgICAgICAgICAgc2VuZChcImxvZ1ZhbHVlXCIsIG1zZyk7XG4gICAgICAgIH1cbiAgICAgICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgICAgICBjb25zb2xlLmxvZyhcIk9wZW5XUE06IFVuc3VjY2Vzc2Z1bCB2YWx1ZSBsb2chXCIpO1xuICAgICAgICAgICAgbG9nRXJyb3JUb0NvbnNvbGUoZXJyb3IpO1xuICAgICAgICB9XG4gICAgICAgIGluTG9nID0gZmFsc2U7XG4gICAgfVxuICAgIC8vIEZvciBmdW5jdGlvbnNcbiAgICBmdW5jdGlvbiBsb2dDYWxsKGluc3RydW1lbnRlZEZ1bmN0aW9uTmFtZSwgYXJncywgYXR0cmlidXRlcywgY2FsbENvbnRleHQsIGxvZ1NldHRpbmdzKSB7XG4gICAgICAgIGlmIChpbkxvZykge1xuICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG4gICAgICAgIGluTG9nID0gdHJ1ZTtcbiAgICAgICAgY29uc3Qgb3ZlckxpbWl0ID0gdXBkYXRlQ291bnRlckFuZENoZWNrSWZPdmVyKGNhbGxDb250ZXh0LnNjcmlwdFVybCwgaW5zdHJ1bWVudGVkRnVuY3Rpb25OYW1lKTtcbiAgICAgICAgaWYgKG92ZXJMaW1pdCkge1xuICAgICAgICAgICAgaW5Mb2cgPSBmYWxzZTtcbiAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgfVxuICAgICAgICB0cnkge1xuICAgICAgICAgICAgLy8gQ29udmVydCBzcGVjaWFsIGFyZ3VtZW50cyBhcnJheSB0byBhIHN0YW5kYXJkIGFycmF5IGZvciBKU09OaWZ5aW5nXG4gICAgICAgICAgICBjb25zdCBzZXJpYWxBcmdzID0gW107XG4gICAgICAgICAgICBmb3IgKGNvbnN0IGFyZyBvZiBhcmdzKSB7XG4gICAgICAgICAgICAgICAgc2VyaWFsQXJncy5wdXNoKHNlcmlhbGl6ZU9iamVjdChhcmcsIGxvZ1NldHRpbmdzLmxvZ0Z1bmN0aW9uc0FzU3RyaW5ncykpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgaWYgKGluc3RydW1lbnRlZEZ1bmN0aW9uTmFtZSA9PT0gJ3dpbmRvdy5kb2N1bWVudC5jcmVhdGVFbGVtZW50Jykge1xuICAgICAgICAgICAgICAgIHZhciBuZXdPYmplY3QgPSBPYmplY3QuYXNzaWduKHt9LCBBcnJheS5mcm9tKGF0dHJpYnV0ZXMsICh7IG5hbWUsIHZhbHVlIH0pID0+ICh7IFtuYW1lXTogdmFsdWUgfSkpKTtcbiAgICAgICAgICAgICAgICBhdHRyaWJ1dGVzID0gbmV3T2JqZWN0O1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgY29uc3QgbXNnID0ge1xuICAgICAgICAgICAgICAgIG9wZXJhdGlvbjogSlNPcGVyYXRpb24uY2FsbCxcbiAgICAgICAgICAgICAgICBzeW1ib2w6IGluc3RydW1lbnRlZEZ1bmN0aW9uTmFtZSxcbiAgICAgICAgICAgICAgICBhcmdzOiBzZXJpYWxBcmdzLFxuICAgICAgICAgICAgICAgIHZhbHVlOiBcIlwiLFxuICAgICAgICAgICAgICAgIGF0dHJpYnV0ZXM6IHNlcmlhbGl6ZU9iamVjdChhdHRyaWJ1dGVzKSxcbiAgICAgICAgICAgICAgICBzY3JpcHRVcmw6IGNhbGxDb250ZXh0LnNjcmlwdFVybCxcbiAgICAgICAgICAgICAgICBzY3JpcHRMaW5lOiBjYWxsQ29udGV4dC5zY3JpcHRMaW5lLFxuICAgICAgICAgICAgICAgIHNjcmlwdENvbDogY2FsbENvbnRleHQuc2NyaXB0Q29sLFxuICAgICAgICAgICAgICAgIGZ1bmNOYW1lOiBjYWxsQ29udGV4dC5mdW5jTmFtZSxcbiAgICAgICAgICAgICAgICBzY3JpcHRMb2NFdmFsOiBjYWxsQ29udGV4dC5zY3JpcHRMb2NFdmFsLFxuICAgICAgICAgICAgICAgIGNhbGxTdGFjazogY2FsbENvbnRleHQuY2FsbFN0YWNrLFxuICAgICAgICAgICAgICAgIG9yZGluYWw6IG9yZGluYWwrKyxcbiAgICAgICAgICAgIH07XG4gICAgICAgICAgICBzZW5kKFwibG9nQ2FsbFwiLCBtc2cpO1xuICAgICAgICB9XG4gICAgICAgIGNhdGNoIChlcnJvcikge1xuICAgICAgICAgICAgY29uc29sZS5sb2coXCJPcGVuV1BNOiBVbnN1Y2Nlc3NmdWwgY2FsbCBsb2c6IFwiICsgaW5zdHJ1bWVudGVkRnVuY3Rpb25OYW1lKTtcbiAgICAgICAgICAgIGxvZ0Vycm9yVG9Db25zb2xlKGVycm9yKTtcbiAgICAgICAgfVxuICAgICAgICBpbkxvZyA9IGZhbHNlO1xuICAgIH1cbiAgICBmdW5jdGlvbiBsb2dFcnJvclRvQ29uc29sZShlcnJvciwgY29udGV4dCA9IGZhbHNlKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoXCJPcGVuV1BNOiBFcnJvciBuYW1lOiBcIiArIGVycm9yLm5hbWUpO1xuICAgICAgICBjb25zb2xlLmVycm9yKFwiT3BlbldQTTogRXJyb3IgbWVzc2FnZTogXCIgKyBlcnJvci5tZXNzYWdlKTtcbiAgICAgICAgY29uc29sZS5lcnJvcihcIk9wZW5XUE06IEVycm9yIGZpbGVuYW1lOiBcIiArIGVycm9yLmZpbGVOYW1lKTtcbiAgICAgICAgY29uc29sZS5lcnJvcihcIk9wZW5XUE06IEVycm9yIGxpbmUgbnVtYmVyOiBcIiArIGVycm9yLmxpbmVOdW1iZXIpO1xuICAgICAgICBjb25zb2xlLmVycm9yKFwiT3BlbldQTTogRXJyb3Igc3RhY2s6IFwiICsgZXJyb3Iuc3RhY2spO1xuICAgICAgICBpZiAoY29udGV4dCkge1xuICAgICAgICAgICAgY29uc29sZS5lcnJvcihcIk9wZW5XUE06IEVycm9yIGNvbnRleHQ6IFwiICsgSlNPTi5zdHJpbmdpZnkoY29udGV4dCkpO1xuICAgICAgICB9XG4gICAgfVxuICAgIC8vIEhlbHBlciB0byBnZXQgb3JpZ2luYXRpbmcgc2NyaXB0IHVybHNcbiAgICBmdW5jdGlvbiBnZXRTdGFja1RyYWNlKCkge1xuICAgICAgICBsZXQgc3RhY2s7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoKTtcbiAgICAgICAgfVxuICAgICAgICBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgICBzdGFjayA9IGVyci5zdGFjaztcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gc3RhY2s7XG4gICAgfVxuICAgIC8vIGZyb20gaHR0cDovL3N0YWNrb3ZlcmZsb3cuY29tL2EvNTIwMjE4NVxuICAgIGNvbnN0IHJzcGxpdCA9IGZ1bmN0aW9uIChzb3VyY2UsIHNlcCwgbWF4c3BsaXQpIHtcbiAgICAgICAgY29uc3Qgc3BsaXQgPSBzb3VyY2Uuc3BsaXQoc2VwKTtcbiAgICAgICAgcmV0dXJuIG1heHNwbGl0XG4gICAgICAgICAgICA/IFtzcGxpdC5zbGljZSgwLCAtbWF4c3BsaXQpLmpvaW4oc2VwKV0uY29uY2F0KHNwbGl0LnNsaWNlKC1tYXhzcGxpdCkpXG4gICAgICAgICAgICA6IHNwbGl0O1xuICAgIH07XG4gICAgZnVuY3Rpb24gZ2V0T3JpZ2luYXRpbmdTY3JpcHRDb250ZXh0KGdldENhbGxTdGFjayA9IGZhbHNlKSB7XG4gICAgICAgIGNvbnN0IHRyYWNlID0gZ2V0U3RhY2tUcmFjZSgpLnRyaW0oKS5zcGxpdChcIlxcblwiKTtcbiAgICAgICAgLy8gcmV0dXJuIGEgY29udGV4dCBvYmplY3QgZXZlbiBpZiB0aGVyZSBpcyBhbiBlcnJvclxuICAgICAgICBjb25zdCBlbXB0eV9jb250ZXh0ID0ge1xuICAgICAgICAgICAgc2NyaXB0VXJsOiBcIlwiLFxuICAgICAgICAgICAgc2NyaXB0TGluZTogXCJcIixcbiAgICAgICAgICAgIHNjcmlwdENvbDogXCJcIixcbiAgICAgICAgICAgIGZ1bmNOYW1lOiBcIlwiLFxuICAgICAgICAgICAgc2NyaXB0TG9jRXZhbDogXCJcIixcbiAgICAgICAgICAgIGNhbGxTdGFjazogXCJcIixcbiAgICAgICAgfTtcbiAgICAgICAgaWYgKHRyYWNlLmxlbmd0aCA8IDQpIHtcbiAgICAgICAgICAgIHJldHVybiBlbXB0eV9jb250ZXh0O1xuICAgICAgICB9XG4gICAgICAgIC8vIDAsIDEgYW5kIDIgYXJlIE9wZW5XUE0ncyBvd24gZnVuY3Rpb25zIChlLmcuIGdldFN0YWNrVHJhY2UpLCBza2lwIHRoZW0uXG4gICAgICAgIGNvbnN0IGNhbGxTaXRlID0gdHJhY2VbM107XG4gICAgICAgIGlmICghY2FsbFNpdGUpIHtcbiAgICAgICAgICAgIHJldHVybiBlbXB0eV9jb250ZXh0O1xuICAgICAgICB9XG4gICAgICAgIC8qXG4gICAgICAgICAqIFN0YWNrIGZyYW1lIGZvcm1hdCBpcyBzaW1wbHk6IEZVTkNfTkFNRUBGSUxFTkFNRTpMSU5FX05POkNPTFVNTl9OT1xuICAgICAgICAgKlxuICAgICAgICAgKiBJZiBldmFsIG9yIEZ1bmN0aW9uIGlzIGludm9sdmVkIHdlIGhhdmUgYW4gYWRkaXRpb25hbCBwYXJ0IGFmdGVyIHRoZSBGSUxFTkFNRSwgZS5nLjpcbiAgICAgICAgICogRlVOQ19OQU1FQEZJTEVOQU1FIGxpbmUgMTIzID4gZXZhbCBsaW5lIDEgPiBldmFsOkxJTkVfTk86Q09MVU1OX05PXG4gICAgICAgICAqIG9yIEZVTkNfTkFNRUBGSUxFTkFNRSBsaW5lIDIzNCA+IEZ1bmN0aW9uOkxJTkVfTk86Q09MVU1OX05PXG4gICAgICAgICAqXG4gICAgICAgICAqIFdlIHN0b3JlIHRoZSBwYXJ0IGJldHdlZW4gdGhlIEZJTEVOQU1FIGFuZCB0aGUgTElORV9OTyBpbiBzY3JpcHRMb2NFdmFsXG4gICAgICAgICAqL1xuICAgICAgICB0cnkge1xuICAgICAgICAgICAgbGV0IHNjcmlwdFVybCA9IFwiXCI7XG4gICAgICAgICAgICBsZXQgc2NyaXB0TG9jRXZhbCA9IFwiXCI7IC8vIGZvciBldmFsIG9yIEZ1bmN0aW9uIGNhbGxzXG4gICAgICAgICAgICBjb25zdCBjYWxsU2l0ZVBhcnRzID0gY2FsbFNpdGUuc3BsaXQoXCJAXCIpO1xuICAgICAgICAgICAgY29uc3QgZnVuY05hbWUgPSBjYWxsU2l0ZVBhcnRzWzBdIHx8IFwiXCI7XG4gICAgICAgICAgICBjb25zdCBpdGVtcyA9IHJzcGxpdChjYWxsU2l0ZVBhcnRzWzFdLCBcIjpcIiwgMik7XG4gICAgICAgICAgICBjb25zdCBjb2x1bW5ObyA9IGl0ZW1zW2l0ZW1zLmxlbmd0aCAtIDFdO1xuICAgICAgICAgICAgY29uc3QgbGluZU5vID0gaXRlbXNbaXRlbXMubGVuZ3RoIC0gMl07XG4gICAgICAgICAgICBjb25zdCBzY3JpcHRGaWxlTmFtZSA9IGl0ZW1zW2l0ZW1zLmxlbmd0aCAtIDNdIHx8IFwiXCI7XG4gICAgICAgICAgICBjb25zdCBsaW5lTm9JZHggPSBzY3JpcHRGaWxlTmFtZS5pbmRleE9mKFwiIGxpbmUgXCIpOyAvLyBsaW5lIGluIHRoZSBVUkwgbWVhbnMgZXZhbCBvciBGdW5jdGlvblxuICAgICAgICAgICAgaWYgKGxpbmVOb0lkeCA9PT0gLTEpIHtcbiAgICAgICAgICAgICAgICBzY3JpcHRVcmwgPSBzY3JpcHRGaWxlTmFtZTsgLy8gVE9ETzogc29tZXRpbWVzIHdlIGhhdmUgZmlsZW5hbWUgb25seSwgZS5nLiBYWC5qc1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgc2NyaXB0VXJsID0gc2NyaXB0RmlsZU5hbWUuc2xpY2UoMCwgbGluZU5vSWR4KTtcbiAgICAgICAgICAgICAgICBzY3JpcHRMb2NFdmFsID0gc2NyaXB0RmlsZU5hbWUuc2xpY2UobGluZU5vSWR4ICsgMSwgc2NyaXB0RmlsZU5hbWUubGVuZ3RoKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGNvbnN0IGNhbGxDb250ZXh0ID0ge1xuICAgICAgICAgICAgICAgIHNjcmlwdFVybCxcbiAgICAgICAgICAgICAgICBzY3JpcHRMaW5lOiBsaW5lTm8sXG4gICAgICAgICAgICAgICAgc2NyaXB0Q29sOiBjb2x1bW5ObyxcbiAgICAgICAgICAgICAgICBmdW5jTmFtZSxcbiAgICAgICAgICAgICAgICBzY3JpcHRMb2NFdmFsLFxuICAgICAgICAgICAgICAgIGNhbGxTdGFjazogZ2V0Q2FsbFN0YWNrID8gdHJhY2Uuc2xpY2UoMykuam9pbihcIlxcblwiKS50cmltKCkgOiBcIlwiLFxuICAgICAgICAgICAgfTtcbiAgICAgICAgICAgIHJldHVybiBjYWxsQ29udGV4dDtcbiAgICAgICAgfVxuICAgICAgICBjYXRjaCAoZSkge1xuICAgICAgICAgICAgY29uc29sZS5sb2coXCJPcGVuV1BNOiBFcnJvciBwYXJzaW5nIHRoZSBzY3JpcHQgY29udGV4dFwiLCBlLnRvU3RyaW5nKCksIGNhbGxTaXRlKTtcbiAgICAgICAgICAgIHJldHVybiBlbXB0eV9jb250ZXh0O1xuICAgICAgICB9XG4gICAgfVxuICAgIGZ1bmN0aW9uIGlzT2JqZWN0KG9iamVjdCwgcHJvcGVydHlOYW1lKSB7XG4gICAgICAgIGxldCBwcm9wZXJ0eTtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgIHByb3BlcnR5ID0gb2JqZWN0W3Byb3BlcnR5TmFtZV07XG4gICAgICAgIH1cbiAgICAgICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgICAgICByZXR1cm4gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKHByb3BlcnR5ID09PSBudWxsKSB7XG4gICAgICAgICAgICAvLyBudWxsIGlzIHR5cGUgXCJvYmplY3RcIlxuICAgICAgICAgICAgcmV0dXJuIGZhbHNlO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiB0eXBlb2YgcHJvcGVydHkgPT09IFwib2JqZWN0XCI7XG4gICAgfVxuICAgIC8vIExvZyBjYWxscyB0byBhIGdpdmVuIGZ1bmN0aW9uXG4gICAgLy8gVGhpcyBoZWxwZXIgZnVuY3Rpb24gcmV0dXJucyBhIHdyYXBwZXIgYXJvdW5kIGBmdW5jYCB3aGljaCBsb2dzIGNhbGxzXG4gICAgLy8gdG8gYGZ1bmNgLiBgb2JqZWN0TmFtZWAgYW5kIGBtZXRob2ROYW1lYCBhcmUgdXNlZCBzdHJpY3RseSB0byBpZGVudGlmeVxuICAgIC8vIHdoaWNoIG9iamVjdCBtZXRob2QgYGZ1bmNgIGlzIGNvbWluZyBmcm9tIGluIHRoZSBsb2dzXG4gICAgZnVuY3Rpb24gaW5zdHJ1bWVudEZ1bmN0aW9uKG9iamVjdE5hbWUsIG1ldGhvZE5hbWUsIGZ1bmMsIGxvZ1NldHRpbmdzKSB7XG4gICAgICAgIHJldHVybiBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICBjb25zdCBjYWxsQ29udGV4dCA9IGdldE9yaWdpbmF0aW5nU2NyaXB0Q29udGV4dChsb2dTZXR0aW5ncy5sb2dDYWxsU3RhY2spO1xuICAgICAgICAgICAgLyoqKioqKioqKioqKioqKioqKioqKi9cbiAgICAgICAgICAgIHZhciBhdHRyaWJ1dGVzID0gXCJcIjtcbiAgICAgICAgICAgIC8vIENoZWNrIGZvciBjcmVhdGlvbiBvZiBlbGVtZW50LiBTZXQgYW4gb3BlbndwbSBhdHRyaWJ1dGUgdG8gaWRlbnRpZnkgdGhpc1xuICAgICAgICAgICAgLy8gZWxlbWVudCBsYXRlci4gXG4gICAgICAgICAgICBpZiAobWV0aG9kTmFtZSA9PSBcImNyZWF0ZUVsZW1lbnRcIikge1xuICAgICAgICAgICAgICAgIC8vdmFyIG9ic2VydmVyID0gY3JlYXRlTXV0YXRpb25PYnNlcnZlcihvYmplY3QsIG9iamVjdE5hbWUsIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgICAgICAvLyBvYmplY3QuYWRkRXZlbnRMaXN0ZW5lcihcIkRPTUF0dHJNb2RpZmllZFwiLCBmdW5jdGlvbiAoZXZlbnQpIHtcbiAgICAgICAgICAgICAgICAvLyBjb25zb2xlLmxvZyhcImluIG1vZGlmeSBsaXN0ZW5lciFcIik7XG4gICAgICAgICAgICAgICAgLy8gc3dpdGNoKGV2ZW50LmF0dHJDaGFuZ2UpIHtcbiAgICAgICAgICAgICAgICAvLyAgIGNhc2UgTXV0YXRpb25FdmVudC5NT0RJRklDQVRJT046XG4gICAgICAgICAgICAgICAgLy8gICAgIGNvbnNvbGUubG9nKFwibW9kaWZpZWQhXCIpO1xuICAgICAgICAgICAgICAgIC8vICAgICB2YXIgbm9kZSA9IGV2ZW50LnRhcmdldC50YWdOYW1lO1xuICAgICAgICAgICAgICAgIC8vICAgICBsb2dDYWxsKG5vZGUgKyBcIi5hdHRyX21vZGlmaWVkXCIsIFwiXCIsIGV2ZW50LnRhcmdldC5hdHRyaWJ1dGVzLCBjYWxsQ29udGV4dCwgbG9nU2V0dGluZ3MpO1xuICAgICAgICAgICAgICAgIC8vICAgICBicmVhaztcbiAgICAgICAgICAgICAgICAvLyAgIGNhc2UgTXV0YXRpb25FdmVudC5BRERJVElPTjpcbiAgICAgICAgICAgICAgICAvLyAgICAgY29uc29sZS5sb2coXCJhZGRlZCFcIik7XG4gICAgICAgICAgICAgICAgLy8gICAgIGxvZ0NhbGwobm9kZSArIFwiLmF0dHJfYWRkZWRcIiwgXCJcIiwgZXZlbnQudGFyZ2V0LmF0dHJpYnV0ZXMsIGNhbGxDb250ZXh0LCBsb2dTZXR0aW5ncyk7XG4gICAgICAgICAgICAgICAgLy8gICAgIGJyZWFrO1xuICAgICAgICAgICAgICAgIC8vICAgY2FzZSBNdXRhdGlvbkV2ZW50LlJFTU9WQUw6XG4gICAgICAgICAgICAgICAgLy8gICAgIGNvbnNvbGUubG9nKFwicmVtb3ZlZCFcIik7XG4gICAgICAgICAgICAgICAgLy8gICAgIGxvZ0NhbGwobm9kZSArIFwiLmF0dHJfcmVtb3ZlZFwiLCBcIlwiLCBldmVudC50YXJnZXQuYXR0cmlidXRlcywgY2FsbENvbnRleHQsIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgICAgICAvLyAgICAgYnJlYWs7XG4gICAgICAgICAgICAgICAgLy8gICB9XG4gICAgICAgICAgICAgICAgLy8gfSwgZmFsc2UpO1xuICAgICAgICAgICAgICAgIHZhciBmdW5jUmVmID0gZnVuYy5hcHBseSh0aGlzLCBhcmd1bWVudHMpO1xuICAgICAgICAgICAgICAgIC8vU2V0dGluZyBhIHJhbmRvbSA1LWRpZ2l0IHRhZyBmb3Igbm93LlxuICAgICAgICAgICAgICAgIHZhciB0YWcgPSBNYXRoLmZsb29yKE1hdGgucmFuZG9tKCkgKiBNYXRoLnBvdygxMCwgNSkpO1xuICAgICAgICAgICAgICAgIGZ1bmNSZWYuc2V0QXR0cmlidXRlKFwib3BlbndwbVwiLCB0YWcpO1xuICAgICAgICAgICAgICAgIGF0dHJpYnV0ZXMgPSBmdW5jUmVmLmF0dHJpYnV0ZXM7XG4gICAgICAgICAgICAgICAgbG9nQ2FsbChvYmplY3ROYW1lICsgJy4nICsgbWV0aG9kTmFtZSwgYXJndW1lbnRzLCBhdHRyaWJ1dGVzLCBjYWxsQ29udGV4dCwgbG9nU2V0dGluZ3MpO1xuICAgICAgICAgICAgICAgIHJldHVybiBmdW5jUmVmO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgLyoqKioqKioqKioqKioqKioqKioqKi9cbiAgICAgICAgICAgIGxvZ0NhbGwob2JqZWN0TmFtZSArIFwiLlwiICsgbWV0aG9kTmFtZSwgYXJndW1lbnRzLCBhdHRyaWJ1dGVzLCBjYWxsQ29udGV4dCwgbG9nU2V0dGluZ3MpO1xuICAgICAgICAgICAgcmV0dXJuIGZ1bmMuYXBwbHkodGhpcywgYXJndW1lbnRzKTtcbiAgICAgICAgfTtcbiAgICB9XG4gICAgLy8gTG9nIHByb3BlcnRpZXMgb2YgcHJvdG90eXBlcyBhbmQgb2JqZWN0c1xuICAgIGZ1bmN0aW9uIGluc3RydW1lbnRPYmplY3RQcm9wZXJ0eShvYmplY3QsIG9iamVjdE5hbWUsIHByb3BlcnR5TmFtZSwgbG9nU2V0dGluZ3MpIHtcbiAgICAgICAgaWYgKCFvYmplY3QgfHxcbiAgICAgICAgICAgICFvYmplY3ROYW1lIHx8XG4gICAgICAgICAgICAhcHJvcGVydHlOYW1lIHx8XG4gICAgICAgICAgICBwcm9wZXJ0eU5hbWUgPT09IFwidW5kZWZpbmVkXCIpIHtcbiAgICAgICAgICAgIHRocm93IG5ldyBFcnJvcihgSW52YWxpZCByZXF1ZXN0IHRvIGluc3RydW1lbnRPYmplY3RQcm9wZXJ0eS5cbiAgICAgICAgT2JqZWN0OiAke29iamVjdH1cbiAgICAgICAgb2JqZWN0TmFtZTogJHtvYmplY3ROYW1lfVxuICAgICAgICBwcm9wZXJ0eU5hbWU6ICR7cHJvcGVydHlOYW1lfVxuICAgICAgICBgKTtcbiAgICAgICAgfVxuICAgICAgICAvLyBTdG9yZSBvcmlnaW5hbCBkZXNjcmlwdG9yIGluIGNsb3N1cmVcbiAgICAgICAgY29uc3QgcHJvcERlc2MgPSBPYmplY3QuZ2V0UHJvcGVydHlEZXNjcmlwdG9yKG9iamVjdCwgcHJvcGVydHlOYW1lKTtcbiAgICAgICAgLy8gUHJvcGVydHkgZGVzY3JpcHRvciBtdXN0IGV4aXN0IHVubGVzcyB3ZSBhcmUgaW5zdHJ1bWVudGluZyBhIG5vbkV4aXN0aW5nIHByb3BlcnR5XG4gICAgICAgIGlmICghcHJvcERlc2MgJiZcbiAgICAgICAgICAgICFsb2dTZXR0aW5ncy5ub25FeGlzdGluZ1Byb3BlcnRpZXNUb0luc3RydW1lbnQuaW5jbHVkZXMocHJvcGVydHlOYW1lKSkge1xuICAgICAgICAgICAgY29uc29sZS5lcnJvcihcIlByb3BlcnR5IGRlc2NyaXB0b3Igbm90IGZvdW5kIGZvclwiLCBvYmplY3ROYW1lLCBwcm9wZXJ0eU5hbWUsIG9iamVjdCk7XG4gICAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cbiAgICAgICAgLy8gUHJvcGVydHkgZGVzY3JpcHRvciBmb3IgdW5kZWZpbmVkIHByb3BlcnRpZXNcbiAgICAgICAgbGV0IHVuZGVmaW5lZFByb3BWYWx1ZTtcbiAgICAgICAgY29uc3QgdW5kZWZpbmVkUHJvcERlc2MgPSB7XG4gICAgICAgICAgICBnZXQ6ICgpID0+IHtcbiAgICAgICAgICAgICAgICByZXR1cm4gdW5kZWZpbmVkUHJvcFZhbHVlO1xuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHNldDogKHZhbHVlKSA9PiB7XG4gICAgICAgICAgICAgICAgdW5kZWZpbmVkUHJvcFZhbHVlID0gdmFsdWU7XG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAgZW51bWVyYWJsZTogZmFsc2UsXG4gICAgICAgIH07XG4gICAgICAgIC8vIEluc3RydW1lbnQgZGF0YSBvciBhY2Nlc3NvciBwcm9wZXJ0eSBkZXNjcmlwdG9yc1xuICAgICAgICBjb25zdCBvcmlnaW5hbEdldHRlciA9IHByb3BEZXNjID8gcHJvcERlc2MuZ2V0IDogdW5kZWZpbmVkUHJvcERlc2MuZ2V0O1xuICAgICAgICBjb25zdCBvcmlnaW5hbFNldHRlciA9IHByb3BEZXNjID8gcHJvcERlc2Muc2V0IDogdW5kZWZpbmVkUHJvcERlc2Muc2V0O1xuICAgICAgICBsZXQgb3JpZ2luYWxWYWx1ZSA9IHByb3BEZXNjID8gcHJvcERlc2MudmFsdWUgOiB1bmRlZmluZWRQcm9wVmFsdWU7XG4gICAgICAgIC8vIFdlIG92ZXJ3cml0ZSBib3RoIGRhdGEgYW5kIGFjY2Vzc29yIHByb3BlcnRpZXMgYXMgYW4gaW5zdHJ1bWVudGVkXG4gICAgICAgIC8vIGFjY2Vzc29yIHByb3BlcnR5XG4gICAgICAgIE9iamVjdC5kZWZpbmVQcm9wZXJ0eShvYmplY3QsIHByb3BlcnR5TmFtZSwge1xuICAgICAgICAgICAgY29uZmlndXJhYmxlOiB0cnVlLFxuICAgICAgICAgICAgZ2V0OiAoZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgICAgIHJldHVybiBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICAgICAgICAgIGxldCBvcmlnUHJvcGVydHk7XG4gICAgICAgICAgICAgICAgICAgIGNvbnN0IGNhbGxDb250ZXh0ID0gZ2V0T3JpZ2luYXRpbmdTY3JpcHRDb250ZXh0KGxvZ1NldHRpbmdzLmxvZ0NhbGxTdGFjayk7XG4gICAgICAgICAgICAgICAgICAgIGNvbnN0IGluc3RydW1lbnRlZFZhcmlhYmxlTmFtZSA9IGAke29iamVjdE5hbWV9LiR7cHJvcGVydHlOYW1lfWA7XG4gICAgICAgICAgICAgICAgICAgIHZhciBhdHRyaWJ1dGVzID0gXCJcIjtcbiAgICAgICAgICAgICAgICAgICAgLy8gZ2V0IG9yaWdpbmFsIHZhbHVlXG4gICAgICAgICAgICAgICAgICAgIGlmICghcHJvcERlc2MpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIC8vIGlmIHVuZGVmaW5lZCBwcm9wZXJ0eVxuICAgICAgICAgICAgICAgICAgICAgICAgb3JpZ1Byb3BlcnR5ID0gdW5kZWZpbmVkUHJvcFZhbHVlO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgIGVsc2UgaWYgKG9yaWdpbmFsR2V0dGVyKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAvLyBpZiBhY2Nlc3NvciBwcm9wZXJ0eVxuICAgICAgICAgICAgICAgICAgICAgICAgb3JpZ1Byb3BlcnR5ID0gb3JpZ2luYWxHZXR0ZXIuY2FsbCh0aGlzKTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICBlbHNlIGlmIChcInZhbHVlXCIgaW4gcHJvcERlc2MpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIC8vIGlmIGRhdGEgcHJvcGVydHlcbiAgICAgICAgICAgICAgICAgICAgICAgIG9yaWdQcm9wZXJ0eSA9IG9yaWdpbmFsVmFsdWU7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBjb25zb2xlLmVycm9yKGBQcm9wZXJ0eSBkZXNjcmlwdG9yIGZvciAke2luc3RydW1lbnRlZFZhcmlhYmxlTmFtZX0gZG9lc24ndCBoYXZlIGdldHRlciBvciB2YWx1ZT9gKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGxvZ1ZhbHVlKGluc3RydW1lbnRlZFZhcmlhYmxlTmFtZSwgXCJcIiwgYXR0cmlidXRlcywgSlNPcGVyYXRpb24uZ2V0X2ZhaWxlZCwgY2FsbENvbnRleHQsIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAvLyBMb2cgYGdldHNgIGV4Y2VwdCB0aG9zZSB0aGF0IGhhdmUgaW5zdHJ1bWVudGVkIHJldHVybiB2YWx1ZXNcbiAgICAgICAgICAgICAgICAgICAgLy8gKiBBbGwgcmV0dXJuZWQgZnVuY3Rpb25zIGFyZSBpbnN0cnVtZW50ZWQgd2l0aCBhIHdyYXBwZXJcbiAgICAgICAgICAgICAgICAgICAgLy8gKiBSZXR1cm5lZCBvYmplY3RzIG1heSBiZSBpbnN0cnVtZW50ZWQgaWYgcmVjdXJzaXZlXG4gICAgICAgICAgICAgICAgICAgIC8vICAgaW5zdHJ1bWVudGF0aW9uIGlzIGVuYWJsZWQgYW5kIHRoaXMgaXNuJ3QgYXQgdGhlIGRlcHRoIGxpbWl0LlxuICAgICAgICAgICAgICAgICAgICBpZiAodHlwZW9mIG9yaWdQcm9wZXJ0eSA9PT0gXCJmdW5jdGlvblwiKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBpZiAobG9nU2V0dGluZ3MubG9nRnVuY3Rpb25HZXRzKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgbG9nVmFsdWUoaW5zdHJ1bWVudGVkVmFyaWFibGVOYW1lLCBvcmlnUHJvcGVydHksIGF0dHJpYnV0ZXMsIEpTT3BlcmF0aW9uLmdldF9mdW5jdGlvbiwgY2FsbENvbnRleHQsIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgIGNvbnN0IGluc3RydW1lbnRlZEZ1bmN0aW9uV3JhcHBlciA9IGluc3RydW1lbnRGdW5jdGlvbihvYmplY3ROYW1lLCBwcm9wZXJ0eU5hbWUsIG9yaWdQcm9wZXJ0eSwgbG9nU2V0dGluZ3MpO1xuICAgICAgICAgICAgICAgICAgICAgICAgLy8gUmVzdG9yZSB0aGUgb3JpZ2luYWwgcHJvdG90eXBlIGFuZCBjb25zdHJ1Y3RvciBzbyB0aGF0IGluc3RydW1lbnRlZCBjbGFzc2VzIHJlbWFpbiBpbnRhY3RcbiAgICAgICAgICAgICAgICAgICAgICAgIC8vIFRPRE86IFRoaXMgbWF5IGhhdmUgaW50cm9kdWNlZCBwcm90b3R5cGUgcG9sbHV0aW9uIGFzIHBlciBodHRwczovL2dpdGh1Yi5jb20vbW96aWxsYS9PcGVuV1BNL2lzc3Vlcy80NzFcbiAgICAgICAgICAgICAgICAgICAgICAgIGlmIChvcmlnUHJvcGVydHkucHJvdG90eXBlKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgaW5zdHJ1bWVudGVkRnVuY3Rpb25XcmFwcGVyLnByb3RvdHlwZSA9IG9yaWdQcm9wZXJ0eS5wcm90b3R5cGU7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgKG9yaWdQcm9wZXJ0eS5wcm90b3R5cGUuY29uc3RydWN0b3IpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaW5zdHJ1bWVudGVkRnVuY3Rpb25XcmFwcGVyLnByb3RvdHlwZS5jb25zdHJ1Y3RvciA9XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBvcmlnUHJvcGVydHkucHJvdG90eXBlLmNvbnN0cnVjdG9yO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBpbnN0cnVtZW50ZWRGdW5jdGlvbldyYXBwZXI7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgZWxzZSBpZiAodHlwZW9mIG9yaWdQcm9wZXJ0eSA9PT0gXCJvYmplY3RcIiAmJlxuICAgICAgICAgICAgICAgICAgICAgICAgbG9nU2V0dGluZ3MucmVjdXJzaXZlICYmXG4gICAgICAgICAgICAgICAgICAgICAgICBsb2dTZXR0aW5ncy5kZXB0aCA+IDApIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBvcmlnUHJvcGVydHk7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBsb2dWYWx1ZShpbnN0cnVtZW50ZWRWYXJpYWJsZU5hbWUsIG9yaWdQcm9wZXJ0eSwgYXR0cmlidXRlcywgSlNPcGVyYXRpb24uZ2V0LCBjYWxsQ29udGV4dCwgbG9nU2V0dGluZ3MpO1xuICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIG9yaWdQcm9wZXJ0eTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH07XG4gICAgICAgICAgICB9KSgpLFxuICAgICAgICAgICAgc2V0OiAoZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgICAgIHJldHVybiBmdW5jdGlvbiAodmFsdWUpIHtcbiAgICAgICAgICAgICAgICAgICAgY29uc3QgY2FsbENvbnRleHQgPSBnZXRPcmlnaW5hdGluZ1NjcmlwdENvbnRleHQobG9nU2V0dGluZ3MubG9nQ2FsbFN0YWNrKTtcbiAgICAgICAgICAgICAgICAgICAgY29uc3QgaW5zdHJ1bWVudGVkVmFyaWFibGVOYW1lID0gYCR7b2JqZWN0TmFtZX0uJHtwcm9wZXJ0eU5hbWV9YDtcbiAgICAgICAgICAgICAgICAgICAgbGV0IHJldHVyblZhbHVlO1xuICAgICAgICAgICAgICAgICAgICB2YXIgYXR0cmlidXRlcyA9IFwiXCI7XG4gICAgICAgICAgICAgICAgICAgIC8vIFByZXZlbnQgc2V0cyBmb3IgZnVuY3Rpb25zIGFuZCBvYmplY3RzIGlmIGVuYWJsZWRcbiAgICAgICAgICAgICAgICAgICAgaWYgKGxvZ1NldHRpbmdzLnByZXZlbnRTZXRzICYmXG4gICAgICAgICAgICAgICAgICAgICAgICAodHlwZW9mIG9yaWdpbmFsVmFsdWUgPT09IFwiZnVuY3Rpb25cIiB8fFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHR5cGVvZiBvcmlnaW5hbFZhbHVlID09PSBcIm9iamVjdFwiKSkge1xuICAgICAgICAgICAgICAgICAgICAgICAgbG9nVmFsdWUoaW5zdHJ1bWVudGVkVmFyaWFibGVOYW1lLCB2YWx1ZSwgYXR0cmlidXRlcywgSlNPcGVyYXRpb24uc2V0X3ByZXZlbnRlZCwgY2FsbENvbnRleHQsIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiB2YWx1ZTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAvLyBzZXQgbmV3IHZhbHVlIHRvIG9yaWdpbmFsIHNldHRlci9sb2NhdGlvblxuICAgICAgICAgICAgICAgICAgICBpZiAob3JpZ2luYWxTZXR0ZXIpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIC8vIGlmIGFjY2Vzc29yIHByb3BlcnR5XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm5WYWx1ZSA9IG9yaWdpbmFsU2V0dGVyLmNhbGwodGhpcywgdmFsdWUpO1xuICAgICAgICAgICAgICAgICAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBpZiAodGhpcy5nZXRBdHRyaWJ1dGUoXCJvcGVud3BtXCIpKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGF0dHJpYnV0ZXMgPSB0aGlzLmF0dHJpYnV0ZXM7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAgICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgY29uc29sZS53YXJuKFwiRXJyb3IgaW4gZ2V0dGluZyBvcGVud3BtIGF0dHJpYnV0ZVwiKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBhdHRyaWJ1dGVzID0gXCJcIjtcbiAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICBlbHNlIGlmIChcInZhbHVlXCIgaW4gcHJvcERlc2MpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGluTG9nID0gdHJ1ZTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGlmIChvYmplY3QuaXNQcm90b3R5cGVPZih0aGlzKSkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIE9iamVjdC5kZWZpbmVQcm9wZXJ0eSh0aGlzLCBwcm9wZXJ0eU5hbWUsIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFsdWUsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICAgICBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBvcmlnaW5hbFZhbHVlID0gdmFsdWU7XG4gICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm5WYWx1ZSA9IHZhbHVlO1xuICAgICAgICAgICAgICAgICAgICAgICAgaW5Mb2cgPSBmYWxzZTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNvbnNvbGUuZXJyb3IoYFByb3BlcnR5IGRlc2NyaXB0b3IgZm9yICR7aW5zdHJ1bWVudGVkVmFyaWFibGVOYW1lfSBkb2Vzbid0IGhhdmUgc2V0dGVyIG9yIHZhbHVlP2ApO1xuICAgICAgICAgICAgICAgICAgICAgICAgbG9nVmFsdWUoaW5zdHJ1bWVudGVkVmFyaWFibGVOYW1lLCB2YWx1ZSwgYXR0cmlidXRlcywgSlNPcGVyYXRpb24uc2V0X2ZhaWxlZCwgY2FsbENvbnRleHQsIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiB2YWx1ZTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICBsb2dWYWx1ZShpbnN0cnVtZW50ZWRWYXJpYWJsZU5hbWUsIHZhbHVlLCBhdHRyaWJ1dGVzLCBKU09wZXJhdGlvbi5zZXQsIGNhbGxDb250ZXh0LCBsb2dTZXR0aW5ncyk7XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiByZXR1cm5WYWx1ZTtcbiAgICAgICAgICAgICAgICB9O1xuICAgICAgICAgICAgfSkoKSxcbiAgICAgICAgfSk7XG4gICAgfVxuICAgIGZ1bmN0aW9uIGluc3RydW1lbnRPYmplY3Qob2JqZWN0LCBpbnN0cnVtZW50ZWROYW1lLCBsb2dTZXR0aW5ncykge1xuICAgICAgICAvLyBTZXQgcHJvcGVydGllc1RvSW5zdHJ1bWVudCB0byBudWxsIHRvIGZvcmNlIG5vIHByb3BlcnRpZXMgdG8gYmUgaW5zdHJ1bWVudGVkLlxuICAgICAgICAvLyAodGhpcyBpcyB1c2VkIGluIHRlc3RpbmcgZm9yIGV4YW1wbGUpXG4gICAgICAgIGxldCBwcm9wZXJ0aWVzVG9JbnN0cnVtZW50O1xuICAgICAgICBpZiAobG9nU2V0dGluZ3MucHJvcGVydGllc1RvSW5zdHJ1bWVudCA9PT0gbnVsbCkge1xuICAgICAgICAgICAgcHJvcGVydGllc1RvSW5zdHJ1bWVudCA9IFtdO1xuICAgICAgICB9XG4gICAgICAgIGVsc2UgaWYgKGxvZ1NldHRpbmdzLnByb3BlcnRpZXNUb0luc3RydW1lbnQubGVuZ3RoID09PSAwKSB7XG4gICAgICAgICAgICBwcm9wZXJ0aWVzVG9JbnN0cnVtZW50ID0gT2JqZWN0LmdldFByb3BlcnR5TmFtZXMob2JqZWN0KTtcbiAgICAgICAgfVxuICAgICAgICBlbHNlIHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQgPSBsb2dTZXR0aW5ncy5wcm9wZXJ0aWVzVG9JbnN0cnVtZW50O1xuICAgICAgICB9XG4gICAgICAgIGZvciAoY29uc3QgcHJvcGVydHlOYW1lIG9mIHByb3BlcnRpZXNUb0luc3RydW1lbnQpIHtcbiAgICAgICAgICAgIGlmIChsb2dTZXR0aW5ncy5leGNsdWRlZFByb3BlcnRpZXMuaW5jbHVkZXMocHJvcGVydHlOYW1lKSkge1xuICAgICAgICAgICAgICAgIGNvbnRpbnVlO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgLy8gSWYgYHJlY3Vyc2l2ZWAgZmxhZyBzZXQgd2Ugd2FudCB0byByZWN1cnNpdmVseSBpbnN0cnVtZW50IGFueVxuICAgICAgICAgICAgLy8gb2JqZWN0IHByb3BlcnRpZXMgdGhhdCBhcmVuJ3QgdGhlIHByb3RvdHlwZSBvYmplY3QuXG4gICAgICAgICAgICBpZiAobG9nU2V0dGluZ3MucmVjdXJzaXZlICYmXG4gICAgICAgICAgICAgICAgbG9nU2V0dGluZ3MuZGVwdGggPiAwICYmXG4gICAgICAgICAgICAgICAgaXNPYmplY3Qob2JqZWN0LCBwcm9wZXJ0eU5hbWUpICYmXG4gICAgICAgICAgICAgICAgcHJvcGVydHlOYW1lICE9PSBcIl9fcHJvdG9fX1wiKSB7XG4gICAgICAgICAgICAgICAgY29uc3QgbmV3SW5zdHJ1bWVudGVkTmFtZSA9IGAke2luc3RydW1lbnRlZE5hbWV9LiR7cHJvcGVydHlOYW1lfWA7XG4gICAgICAgICAgICAgICAgY29uc3QgbmV3TG9nU2V0dGluZ3MgPSB7IC4uLmxvZ1NldHRpbmdzIH07XG4gICAgICAgICAgICAgICAgbmV3TG9nU2V0dGluZ3MuZGVwdGggPSBsb2dTZXR0aW5ncy5kZXB0aCAtIDE7XG4gICAgICAgICAgICAgICAgbmV3TG9nU2V0dGluZ3MucHJvcGVydGllc1RvSW5zdHJ1bWVudCA9IFtdO1xuICAgICAgICAgICAgICAgIGluc3RydW1lbnRPYmplY3Qob2JqZWN0W3Byb3BlcnR5TmFtZV0sIG5ld0luc3RydW1lbnRlZE5hbWUsIG5ld0xvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIHRyeSB7XG4gICAgICAgICAgICAgICAgaW5zdHJ1bWVudE9iamVjdFByb3BlcnR5KG9iamVjdCwgaW5zdHJ1bWVudGVkTmFtZSwgcHJvcGVydHlOYW1lLCBsb2dTZXR0aW5ncyk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBjYXRjaCAoZXJyb3IpIHtcbiAgICAgICAgICAgICAgICBpZiAoZXJyb3IgaW5zdGFuY2VvZiBUeXBlRXJyb3IgJiZcbiAgICAgICAgICAgICAgICAgICAgZXJyb3IubWVzc2FnZS5pbmNsdWRlcyhcImNhbid0IHJlZGVmaW5lIG5vbi1jb25maWd1cmFibGUgcHJvcGVydHlcIikpIHtcbiAgICAgICAgICAgICAgICAgICAgY29uc29sZS53YXJuKGBDYW5ub3QgaW5zdHJ1bWVudCBub24tY29uZmlndXJhYmxlIHByb3BlcnR5OiAke2luc3RydW1lbnRlZE5hbWV9OiR7cHJvcGVydHlOYW1lfWApO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgbG9nRXJyb3JUb0NvbnNvbGUoZXJyb3IsIHsgaW5zdHJ1bWVudGVkTmFtZSwgcHJvcGVydHlOYW1lIH0pO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICBmb3IgKGNvbnN0IHByb3BlcnR5TmFtZSBvZiBsb2dTZXR0aW5ncy5ub25FeGlzdGluZ1Byb3BlcnRpZXNUb0luc3RydW1lbnQpIHtcbiAgICAgICAgICAgIGlmIChsb2dTZXR0aW5ncy5leGNsdWRlZFByb3BlcnRpZXMuaW5jbHVkZXMocHJvcGVydHlOYW1lKSkge1xuICAgICAgICAgICAgICAgIGNvbnRpbnVlO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgICAgICBpbnN0cnVtZW50T2JqZWN0UHJvcGVydHkob2JqZWN0LCBpbnN0cnVtZW50ZWROYW1lLCBwcm9wZXJ0eU5hbWUsIGxvZ1NldHRpbmdzKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGNhdGNoIChlcnJvcikge1xuICAgICAgICAgICAgICAgIGxvZ0Vycm9yVG9Db25zb2xlKGVycm9yLCB7IGluc3RydW1lbnRlZE5hbWUsIHByb3BlcnR5TmFtZSB9KTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgIH1cbiAgICBjb25zdCBzZW5kRmFjdG9yeSA9IGZ1bmN0aW9uIChldmVudElkLCAkc2VuZE1lc3NhZ2VzVG9Mb2dnZXIpIHtcbiAgICAgICAgbGV0IG1lc3NhZ2VzID0gW107XG4gICAgICAgIC8vIGRlYm91bmNlIHNlbmRpbmcgcXVldWVkIG1lc3NhZ2VzXG4gICAgICAgIGNvbnN0IF9zZW5kID0gZGVib3VuY2UoZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgJHNlbmRNZXNzYWdlc1RvTG9nZ2VyKGV2ZW50SWQsIG1lc3NhZ2VzKTtcbiAgICAgICAgICAgIC8vIGNsZWFyIHRoZSBxdWV1ZVxuICAgICAgICAgICAgbWVzc2FnZXMgPSBbXTtcbiAgICAgICAgfSwgMTAwKTtcbiAgICAgICAgcmV0dXJuIGZ1bmN0aW9uIChtc2dUeXBlLCBtc2cpIHtcbiAgICAgICAgICAgIC8vIHF1ZXVlIHRoZSBtZXNzYWdlXG4gICAgICAgICAgICBtZXNzYWdlcy5wdXNoKHsgdHlwZTogbXNnVHlwZSwgY29udGVudDogbXNnIH0pO1xuICAgICAgICAgICAgX3NlbmQoKTtcbiAgICAgICAgfTtcbiAgICB9O1xuICAgIGNvbnN0IHNlbmQgPSBzZW5kRmFjdG9yeShldmVudElkLCBzZW5kTWVzc2FnZXNUb0xvZ2dlcik7XG4gICAgZnVuY3Rpb24gaW5zdHJ1bWVudEpTKEpTSW5zdHJ1bWVudFJlcXVlc3RzKSB7XG4gICAgICAgIC8vIFRoZSBKUyBJbnN0cnVtZW50IFJlcXVlc3RzIGFyZSBzZXR1cCBhbmQgdmFsaWRhdGVkIHB5dGhvbiBzaWRlXG4gICAgICAgIC8vIGluY2x1ZGluZyBzZXR0aW5nIGRlZmF1bHRzIGZvciBsb2dTZXR0aW5ncy5cbiAgICAgICAgLy8gTW9yZSBkZXRhaWxzIGFib3V0IGhvdyB0aGlzIGZ1bmN0aW9uIGlzIGludm9rZWQgYXJlIGluXG4gICAgICAgIC8vIGNvbnRlbnQvamF2YXNjcmlwdC1pbnN0cnVtZW50LWNvbnRlbnQtc2NvcGUudHNcbiAgICAgICAgSlNJbnN0cnVtZW50UmVxdWVzdHMuZm9yRWFjaChmdW5jdGlvbiAoaXRlbSkge1xuICAgICAgICAgICAgaW5zdHJ1bWVudE9iamVjdChldmFsKGl0ZW0ub2JqZWN0KSwgaXRlbS5pbnN0cnVtZW50ZWROYW1lLCBpdGVtLmxvZ1NldHRpbmdzKTtcbiAgICAgICAgfSk7XG4gICAgfVxuICAgIC8vIFRoaXMgd2hvbGUgZnVuY3Rpb24gZ2V0SW5zdHJ1bWVudEpTIHJldHVybnMganVzdCB0aGUgZnVuY3Rpb24gYGluc3RydW1lbnRKU2AuXG4gICAgcmV0dXJuIGluc3RydW1lbnRKUztcbn1cbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWFuTXRhVzV6ZEhKMWJXVnVkSE11YW5NaUxDSnpiM1Z5WTJWU2IyOTBJam9pSWl3aWMyOTFjbU5sY3lJNld5SXVMaTh1TGk4dUxpOXpjbU12YkdsaUwycHpMV2x1YzNSeWRXMWxiblJ6TG5SeklsMHNJbTVoYldWeklqcGJYU3dpYldGd2NHbHVaM01pT2lKQlFVRkJMR2xGUVVGcFJUdEJRVU5xUlN4dlJrRkJiMFk3UVVFNFFuQkdMRTFCUVUwc1ZVRkJWU3hsUVVGbExFTkJRVU1zVDBGQlpTeEZRVUZGTEc5Q1FVRnZRanRKUVVOdVJUczdPMDlCUjBjN1NVRkZTQ3gzUTBGQmQwTTdTVUZEZUVNc2IwTkJRVzlETzBsQlEzQkRMSEZEUVVGeFF6dEpRVU55UXl4cFEwRkJhVU03U1VGRGFrTXNhVU5CUVdsRE8wbEJRMnBETEhGQ1FVRnhRanRKUVVOeVFpeGxRVUZsTzBsQlEyWXNUVUZCVFR0SlFVVk9MRzFGUVVGdFJUdEpRVU51UlN4TlFVRk5MRmRCUVZjc1IwRkJSeXhIUVVGSExFTkJRVU03U1VGRGVFSXNZVUZCWVR0SlFVTmlMRTFCUVUwc1ZVRkJWU3hIUVVGSExFbEJRVWtzVFVGQlRTeEZRVUZGTEVOQlFVTTdTVUZEYUVNc0swTkJRU3RETzBsQlF5OURMRWxCUVVrc1MwRkJTeXhIUVVGSExFdEJRVXNzUTBGQlF6dEpRVU5zUWl4blJFRkJaMFE3U1VGRGFFUXNTVUZCU1N4UFFVRlBMRWRCUVVjc1EwRkJReXhEUVVGRE8wbEJSV2hDTERCQ1FVRXdRanRKUVVNeFFpeE5RVUZOTEZkQlFWY3NSMEZCUnp0UlFVTnNRaXhKUVVGSkxFVkJRVVVzVFVGQlRUdFJRVU5hTEVkQlFVY3NSVUZCUlN4TFFVRkxPMUZCUTFZc1ZVRkJWU3hGUVVGRkxHRkJRV0U3VVVGRGVrSXNXVUZCV1N4RlFVRkZMR1ZCUVdVN1VVRkROMElzUjBGQlJ5eEZRVUZGTEV0QlFVczdVVUZEVml4VlFVRlZMRVZCUVVVc1lVRkJZVHRSUVVONlFpeGhRVUZoTEVWQlFVVXNaMEpCUVdkQ08wdEJRMmhETEVOQlFVTTdTVUZGUml4dlJrRkJiMFk3U1VGRGNFWXNlVVZCUVhsRk8wbEJRM3BGTEUxQlFVMHNRMEZCUXl4eFFrRkJjVUlzUjBGQlJ5eFZRVUZWTEU5QlFVOHNSVUZCUlN4SlFVRkpPMUZCUTNCRUxFbEJRVWtzVDBGQlR5eExRVUZMTEZOQlFWTXNSVUZCUlR0WlFVTjZRaXhOUVVGTkxFbEJRVWtzUzBGQlN5eERRVUZETERaRFFVRTJReXhEUVVGRExFTkJRVU03VTBGRGFFVTdVVUZEUkN4SlFVRkpMRVZCUVVVc1IwRkJSeXhOUVVGTkxFTkJRVU1zZDBKQlFYZENMRU5CUVVNc1QwRkJUeXhGUVVGRkxFbEJRVWtzUTBGQlF5eERRVUZETzFGQlEzaEVMRWxCUVVrc1MwRkJTeXhIUVVGSExFMUJRVTBzUTBGQlF5eGpRVUZqTEVOQlFVTXNUMEZCVHl4RFFVRkRMRU5CUVVNN1VVRkRNME1zVDBGQlR5eEZRVUZGTEV0QlFVc3NVMEZCVXl4SlFVRkpMRXRCUVVzc1MwRkJTeXhKUVVGSkxFVkJRVVU3V1VGRGVrTXNSVUZCUlN4SFFVRkhMRTFCUVUwc1EwRkJReXgzUWtGQmQwSXNRMEZCUXl4TFFVRkxMRVZCUVVVc1NVRkJTU3hEUVVGRExFTkJRVU03V1VGRGJFUXNTMEZCU3l4SFFVRkhMRTFCUVUwc1EwRkJReXhqUVVGakxFTkJRVU1zUzBGQlN5eERRVUZETEVOQlFVTTdVMEZEZEVNN1VVRkRSQ3hQUVVGUExFVkJRVVVzUTBGQlF6dEpRVU5hTEVOQlFVTXNRMEZCUXp0SlFVVkdMRTFCUVUwc1EwRkJReXhuUWtGQlowSXNSMEZCUnl4VlFVRlZMRTlCUVU4N1VVRkRla01zU1VGQlNTeFBRVUZQTEV0QlFVc3NVMEZCVXl4RlFVRkZPMWxCUTNwQ0xFMUJRVTBzU1VGQlNTeExRVUZMTEVOQlFVTXNkME5CUVhkRExFTkJRVU1zUTBGQlF6dFRRVU16UkR0UlFVTkVMRWxCUVVrc1MwRkJTeXhIUVVGSExFMUJRVTBzUTBGQlF5eHRRa0ZCYlVJc1EwRkJReXhQUVVGUExFTkJRVU1zUTBGQlF6dFJRVU5vUkN4SlFVRkpMRXRCUVVzc1IwRkJSeXhOUVVGTkxFTkJRVU1zWTBGQll5eERRVUZETEU5QlFVOHNRMEZCUXl4RFFVRkRPMUZCUXpORExFOUJRVThzUzBGQlN5eExRVUZMTEVsQlFVa3NSVUZCUlR0WlFVTnlRaXhMUVVGTExFZEJRVWNzUzBGQlN5eERRVUZETEUxQlFVMHNRMEZCUXl4TlFVRk5MRU5CUVVNc2JVSkJRVzFDTEVOQlFVTXNTMEZCU3l4RFFVRkRMRU5CUVVNc1EwRkJRenRaUVVONFJDeExRVUZMTEVkQlFVY3NUVUZCVFN4RFFVRkRMR05CUVdNc1EwRkJReXhMUVVGTExFTkJRVU1zUTBGQlF6dFRRVU4wUXp0UlFVTkVMRzlFUVVGdlJEdFJRVU53UkN4UFFVRlBMRXRCUVVzc1EwRkJRenRKUVVObUxFTkJRVU1zUTBGQlF6dEpRVVZHTEc5RFFVRnZRenRKUVVOd1F5eFRRVUZUTEZGQlFWRXNRMEZCUXl4SlFVRkpMRVZCUVVVc1NVRkJTU3hGUVVGRkxGbEJRWEZDTEV0QlFVczdVVUZEZEVRc1NVRkJTU3hQUVVGUExFTkJRVU03VVVGRFdpeEpRVUZKTEVsQlFVa3NRMEZCUXp0UlFVTlVMRWxCUVVrc1QwRkJUeXhEUVVGRE8xRkJRMW9zU1VGQlNTeFRRVUZUTEVOQlFVTTdVVUZEWkN4SlFVRkpMRTFCUVUwc1EwRkJRenRSUVVWWUxFMUJRVTBzUzBGQlN5eEhRVUZITzFsQlExb3NUVUZCVFN4SlFVRkpMRWRCUVVjc1NVRkJTU3hEUVVGRExFZEJRVWNzUlVGQlJTeEhRVUZITEZOQlFWTXNRMEZCUXp0WlFVTndReXhKUVVGSkxFbEJRVWtzUjBGQlJ5eEpRVUZKTEVWQlFVVTdaMEpCUTJZc1QwRkJUeXhIUVVGSExGVkJRVlVzUTBGQlF5eExRVUZMTEVWQlFVVXNTVUZCU1N4SFFVRkhMRWxCUVVrc1EwRkJReXhEUVVGRE8yRkJRekZETzJsQ1FVRk5PMmRDUVVOTUxFOUJRVThzUjBGQlJ5eEpRVUZKTEVOQlFVTTdaMEpCUTJZc1NVRkJTU3hEUVVGRExGTkJRVk1zUlVGQlJUdHZRa0ZEWkN4TlFVRk5MRWRCUVVjc1NVRkJTU3hEUVVGRExFdEJRVXNzUTBGQlF5eFBRVUZQTEVWQlFVVXNTVUZCU1N4RFFVRkRMRU5CUVVNN2IwSkJRMjVETEU5QlFVOHNSMEZCUnl4SlFVRkpMRWRCUVVjc1NVRkJTU3hEUVVGRE8ybENRVU4yUWp0aFFVTkdPMUZCUTBnc1EwRkJReXhEUVVGRE8xRkJSVVlzVDBGQlR6dFpRVU5NTEU5QlFVOHNSMEZCUnl4SlFVRkpMRU5CUVVNN1dVRkRaaXhKUVVGSkxFZEJRVWNzVTBGQlV5eERRVUZETzFsQlEycENMRk5CUVZNc1IwRkJSeXhKUVVGSkxFTkJRVU1zUjBGQlJ5eEZRVUZGTEVOQlFVTTdXVUZEZGtJc1RVRkJUU3hQUVVGUExFZEJRVWNzVTBGQlV5eEpRVUZKTEVOQlFVTXNUMEZCVHl4RFFVRkRPMWxCUTNSRExFbEJRVWtzUTBGQlF5eFBRVUZQTEVWQlFVVTdaMEpCUTFvc1QwRkJUeXhIUVVGSExGVkJRVlVzUTBGQlF5eExRVUZMTEVWQlFVVXNTVUZCU1N4RFFVRkRMRU5CUVVNN1lVRkRia003V1VGRFJDeEpRVUZKTEU5QlFVOHNSVUZCUlR0blFrRkRXQ3hOUVVGTkxFZEJRVWNzU1VGQlNTeERRVUZETEV0QlFVc3NRMEZCUXl4UFFVRlBMRVZCUVVVc1NVRkJTU3hEUVVGRExFTkJRVU03WjBKQlEyNURMRTlCUVU4c1IwRkJSeXhKUVVGSkxFZEJRVWNzU1VGQlNTeERRVUZETzJGQlEzWkNPMWxCUlVRc1QwRkJUeXhOUVVGTkxFTkJRVU03VVVGRGFFSXNRMEZCUXl4RFFVRkRPMGxCUTBvc1EwRkJRenRKUVVWRUxEaERRVUU0UXp0SlFVTTVReXhUUVVGVExHMUNRVUZ0UWl4RFFVRkRMRTlCUVZrc1JVRkJSU3hwUWtGQk1FSXNTMEZCU3p0UlFVTjRSU3hKUVVGSkxFOUJRVThzUzBGQlN5eFJRVUZSTEVOQlFVTXNTVUZCU1N4RlFVRkZPMWxCUXpkQ0xFOUJRVThzVDBGQlR5eERRVUZETEU5QlFVOHNRMEZCUXp0VFFVTjRRanRSUVVORUxFbEJRVWtzVDBGQlR5eERRVUZETEZWQlFWVXNTMEZCU3l4SlFVRkpMRVZCUVVVN1dVRkRMMElzVDBGQlR5eFBRVUZQTEVkQlFVY3NUMEZCVHl4RFFVRkRMRTlCUVU4c1EwRkJRenRUUVVOc1F6dFJRVVZFTEVsQlFVa3NXVUZCV1N4SFFVRkhMRU5CUVVNc1EwRkJRenRSUVVOeVFpeE5RVUZOTEZGQlFWRXNSMEZCUnl4UFFVRlBMRU5CUVVNc1ZVRkJWU3hEUVVGRExGVkJRVlVzUTBGQlF6dFJRVU12UXl4TFFVRkxMRWxCUVVrc1EwRkJReXhIUVVGSExFTkJRVU1zUlVGQlJTeERRVUZETEVkQlFVY3NVVUZCVVN4RFFVRkRMRTFCUVUwc1JVRkJSU3hEUVVGRExFVkJRVVVzUlVGQlJUdFpRVU40UXl4TlFVRk5MRTlCUVU4c1IwRkJSeXhSUVVGUkxFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTTdXVUZETlVJc1NVRkJTU3hQUVVGUExFdEJRVXNzVDBGQlR5eEZRVUZGTzJkQ1FVTjJRaXhKUVVGSkxFbEJRVWtzUjBGQlJ5eHRRa0ZCYlVJc1EwRkJReXhQUVVGUExFTkJRVU1zVlVGQlZTeEZRVUZGTEdOQlFXTXNRMEZCUXl4RFFVRkRPMmRDUVVOdVJTeEpRVUZKTEVsQlFVa3NSMEZCUnl4SFFVRkhMRTlCUVU4c1EwRkJReXhQUVVGUExFZEJRVWNzUjBGQlJ5eEhRVUZITEZsQlFWa3NRMEZCUXp0blFrRkRia1FzU1VGQlNTeEpRVUZKTEVkQlFVY3NSMEZCUnl4UFFVRlBMRU5CUVVNc1JVRkJSU3hEUVVGRE8yZENRVU42UWl4SlFVRkpMRWxCUVVrc1IwRkJSeXhIUVVGSExFOUJRVThzUTBGQlF5eFRRVUZUTEVOQlFVTTdaMEpCUTJoRExFbEJRVWtzWTBGQll5eEZRVUZGTzI5Q1FVTnNRaXhKUVVGSkxFbEJRVWtzUjBGQlJ5eEhRVUZITEU5QlFVOHNRMEZCUXl4TlFVRk5MRU5CUVVNN2IwSkJRemRDTEVsQlFVa3NTVUZCU1N4SFFVRkhMRWRCUVVjc1QwRkJUeXhEUVVGRExFdEJRVXNzUTBGQlF5eFBRVUZQTEVOQlFVTTdiMEpCUTNCRExFbEJRVWtzU1VGQlNTeEhRVUZITEVkQlFVY3NUMEZCVHl4RFFVRkRMRXRCUVVzc1EwRkJReXhWUVVGVkxFTkJRVU03YVVKQlEzaERPMmRDUVVORUxFbEJRVWtzVDBGQlR5eERRVUZETEU5QlFVOHNTMEZCU3l4SFFVRkhMRVZCUVVVN2IwSkJRek5DTEVsQlFVa3NTVUZCU1N4SFFVRkhMRWRCUVVjc1QwRkJUeXhEUVVGRExFbEJRVWtzUTBGQlF6dHBRa0ZETlVJN1owSkJRMFFzU1VGQlNTeEpRVUZKTEVkQlFVY3NRMEZCUXp0blFrRkRXaXhQUVVGUExFbEJRVWtzUTBGQlF6dGhRVU5pTzFsQlEwUXNTVUZCU1N4UFFVRlBMRU5CUVVNc1VVRkJVU3hMUVVGTExFTkJRVU1zU1VGQlNTeFBRVUZQTEVOQlFVTXNUMEZCVHl4TFFVRkxMRTlCUVU4c1EwRkJReXhQUVVGUExFVkJRVVU3WjBKQlEycEZMRmxCUVZrc1JVRkJSU3hEUVVGRE8yRkJRMmhDTzFOQlEwWTdTVUZEU0N4RFFVRkRPMGxCUlVRc1owTkJRV2RETzBsQlEyaERMRk5CUVZNc1pVRkJaU3hEUVVOMFFpeE5RVUZOTEVWQlEwNHNjVUpCUVRoQ0xFdEJRVXM3VVVGRmJrTXNORUpCUVRSQ08xRkJRelZDTEVsQlFVazdXVUZEUml4SlFVRkpMRTFCUVUwc1MwRkJTeXhKUVVGSkxFVkJRVVU3WjBKQlEyNUNMRTlCUVU4c1RVRkJUU3hEUVVGRE8yRkJRMlk3V1VGRFJDeEpRVUZKTEU5QlFVOHNUVUZCVFN4TFFVRkxMRlZCUVZVc1JVRkJSVHRuUWtGRGFFTXNUMEZCVHl4clFrRkJhMElzUTBGQlF5eERRVUZETEVOQlFVTXNUVUZCVFN4RFFVRkRMRkZCUVZFc1JVRkJSU3hEUVVGRExFTkJRVU1zUTBGQlF5eFZRVUZWTEVOQlFVTTdZVUZETlVRN1dVRkRSQ3hKUVVGSkxFOUJRVThzVFVGQlRTeExRVUZMTEZGQlFWRXNSVUZCUlR0blFrRkRPVUlzVDBGQlR5eE5RVUZOTEVOQlFVTTdZVUZEWmp0WlFVTkVMRTFCUVUwc1YwRkJWeXhIUVVGSExFVkJRVVVzUTBGQlF6dFpRVU4yUWl4UFFVRlBMRWxCUVVrc1EwRkJReXhUUVVGVExFTkJRVU1zVFVGQlRTeEZRVUZGTEZWQlFWVXNSMEZCUnl4RlFVRkZMRXRCUVVzN1owSkJRMmhFTEVsQlFVa3NTMEZCU3l4TFFVRkxMRWxCUVVrc1JVRkJSVHR2UWtGRGJFSXNUMEZCVHl4TlFVRk5MRU5CUVVNN2FVSkJRMlk3WjBKQlEwUXNTVUZCU1N4UFFVRlBMRXRCUVVzc1MwRkJTeXhWUVVGVkxFVkJRVVU3YjBKQlF5OUNMRTlCUVU4c2EwSkJRV3RDTEVOQlFVTXNRMEZCUXl4RFFVRkRMRXRCUVVzc1EwRkJReXhSUVVGUkxFVkJRVVVzUTBGQlF5eERRVUZETEVOQlFVTXNWVUZCVlN4RFFVRkRPMmxDUVVNelJEdG5Ra0ZEUkN4SlFVRkpMRTlCUVU4c1MwRkJTeXhMUVVGTExGRkJRVkVzUlVGQlJUdHZRa0ZETjBJc2NVTkJRWEZETzI5Q1FVTnlReXhKUVVGSkxHbENRVUZwUWl4SlFVRkpMRXRCUVVzc1JVRkJSVHQzUWtGRE9VSXNTMEZCU3l4SFFVRkhMRXRCUVVzc1EwRkJReXhsUVVGbExFTkJRVU03Y1VKQlF5OUNPMjlDUVVWRUxIbENRVUY1UWp0dlFrRkRla0lzU1VGQlNTeExRVUZMTEZsQlFWa3NWMEZCVnl4RlFVRkZPM2RDUVVOb1F5eFBRVUZQTEcxQ1FVRnRRaXhEUVVGRExFdEJRVXNzUTBGQlF5eERRVUZETzNGQ1FVTnVRenR2UWtGRlJDd3JRa0ZCSzBJN2IwSkJReTlDTEVsQlFVa3NSMEZCUnl4TFFVRkxMRVZCUVVVc1NVRkJTU3hYUVVGWExFTkJRVU1zVDBGQlR5eERRVUZETEV0QlFVc3NRMEZCUXl4SFFVRkhMRU5CUVVNc1JVRkJSVHQzUWtGRGFFUXNWMEZCVnl4RFFVRkRMRWxCUVVrc1EwRkJReXhMUVVGTExFTkJRVU1zUTBGQlF6dDNRa0ZEZUVJc1QwRkJUeXhMUVVGTExFTkJRVU03Y1VKQlEyUTdlVUpCUVUwN2QwSkJRMHdzVDBGQlR5eFBRVUZQTEV0QlFVc3NRMEZCUXp0eFFrRkRja0k3YVVKQlEwWTdaMEpCUTBRc1QwRkJUeXhMUVVGTExFTkJRVU03V1VGRFppeERRVUZETEVOQlFVTXNRMEZCUXp0VFFVTktPMUZCUVVNc1QwRkJUeXhMUVVGTExFVkJRVVU3V1VGRFpDeFBRVUZQTEVOQlFVTXNSMEZCUnl4RFFVRkRMR2REUVVGblF5eEhRVUZITEV0QlFVc3NRMEZCUXl4RFFVRkRPMWxCUTNSRUxFOUJRVThzZFVKQlFYVkNMRWRCUVVjc1MwRkJTeXhEUVVGRE8xTkJRM2hETzBsQlEwZ3NRMEZCUXp0SlFVVkVMRk5CUVZNc01rSkJRVEpDTEVOQlFVTXNVMEZCVXl4RlFVRkZMRTFCUVUwN1VVRkRjRVFzVFVGQlRTeEhRVUZITEVkQlFVY3NVMEZCVXl4SFFVRkhMRWRCUVVjc1IwRkJSeXhOUVVGTkxFTkJRVU03VVVGRGNrTXNTVUZCU1N4SFFVRkhMRWxCUVVrc1ZVRkJWU3hKUVVGSkxGVkJRVlVzUTBGQlF5eEhRVUZITEVOQlFVTXNTVUZCU1N4WFFVRlhMRVZCUVVVN1dVRkRka1FzVDBGQlR5eEpRVUZKTEVOQlFVTTdVMEZEWWp0aFFVRk5MRWxCUVVrc1EwRkJReXhEUVVGRExFZEJRVWNzU1VGQlNTeFZRVUZWTEVOQlFVTXNSVUZCUlR0WlFVTXZRaXhWUVVGVkxFTkJRVU1zUjBGQlJ5eERRVUZETEVkQlFVY3NRMEZCUXl4RFFVRkRPMU5CUTNKQ08yRkJRVTA3V1VGRFRDeFZRVUZWTEVOQlFVTXNSMEZCUnl4RFFVRkRMRWxCUVVrc1EwRkJReXhEUVVGRE8xTkJRM1JDTzFGQlEwUXNUMEZCVHl4TFFVRkxMRU5CUVVNN1NVRkRaaXhEUVVGRE8wbEJSVVFzZVVOQlFYbERPMGxCUTNwRExGTkJRVk1zVVVGQlVTeERRVU5tTEhkQ1FVRm5ReXhGUVVOb1F5eExRVUZWTEVWQlExWXNWVUZCWlN4RlFVTm1MRk5CUVdsQ0xFVkJRVVVzYVVOQlFXbERPMGxCUTNCRUxGZEJRV2RDTEVWQlEyaENMRmRCUVhkQ08xRkJSWGhDTEVsQlFVa3NTMEZCU3l4RlFVRkZPMWxCUTFRc1QwRkJUenRUUVVOU08xRkJRMFFzUzBGQlN5eEhRVUZITEVsQlFVa3NRMEZCUXp0UlFVVmlMRTFCUVUwc1UwRkJVeXhIUVVGSExESkNRVUV5UWl4RFFVTXpReXhYUVVGWExFTkJRVU1zVTBGQlV5eEZRVU55UWl4M1FrRkJkMElzUTBGRGVrSXNRMEZCUXp0UlFVTkdMRWxCUVVrc1UwRkJVeXhGUVVGRk8xbEJRMklzUzBGQlN5eEhRVUZITEV0QlFVc3NRMEZCUXp0WlFVTmtMRTlCUVU4N1UwRkRVanRSUVVWRUxHbEdRVUZwUmp0UlFVTnFSaXgzUTBGQmQwTTdVVUZEZUVNc1NVRkJTU3gzUWtGQmQwSXNRMEZCUXl4UlFVRlJMRU5CUVVNc1lVRkJZU3hEUVVGRExFVkJRVVU3V1VGRGNFUXNTVUZCU1N4WFFVRlhMRWRCUVVjc1NVRkJTU3hIUVVGSExFTkJRVU1zUzBGQlN5eEZRVUZGTEUxQlFVMHNRMEZCUXl4UlFVRlJMRU5CUVVNc1NVRkJTU3hEUVVGRExFTkJRVU03V1VGRGRrUXNTMEZCU3l4SFFVRkhMRmRCUVZjc1EwRkJReXhKUVVGSkxFTkJRVU03V1VGRGVrSXNTVUZCU1N4VFFVRlRMRWRCUVVjc1RVRkJUU3hEUVVGRExFMUJRVTBzUTBGQlF5eEZRVUZGTEVWQlFVVXNTMEZCU3l4RFFVRkRMRWxCUVVrc1EwRkJReXhWUVVGVkxFVkJRVVVzUTBGQlF5eEZRVUZETEVsQlFVa3NSVUZCUlN4TFFVRkxMRVZCUVVNc1JVRkJSU3hGUVVGRkxFTkJRVU1zUTBGQlF5eEZRVUZETEVOQlFVTXNTVUZCU1N4RFFVRkRMRVZCUVVVc1MwRkJTeXhGUVVGRExFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTTdXVUZEYUVjc1ZVRkJWU3hIUVVGSExGTkJRVk1zUTBGQlF6dFRRVU40UWp0UlFVZEVMRTFCUVUwc1IwRkJSeXhIUVVGSE8xbEJRMVlzVTBGQlV6dFpRVU5VTEUxQlFVMHNSVUZCUlN4M1FrRkJkMEk3V1VGRGFFTXNTMEZCU3l4RlFVRkZMR1ZCUVdVc1EwRkJReXhMUVVGTExFVkJRVVVzVjBGQlZ5eERRVUZETEhGQ1FVRnhRaXhEUVVGRE8xbEJRMmhGTEZWQlFWVXNSVUZCUlN4bFFVRmxMRU5CUVVNc1ZVRkJWU3hEUVVGRE8xbEJRM1pETEZOQlFWTXNSVUZCUlN4WFFVRlhMRU5CUVVNc1UwRkJVenRaUVVOb1F5eFZRVUZWTEVWQlFVVXNWMEZCVnl4RFFVRkRMRlZCUVZVN1dVRkRiRU1zVTBGQlV5eEZRVUZGTEZkQlFWY3NRMEZCUXl4VFFVRlRPMWxCUTJoRExGRkJRVkVzUlVGQlJTeFhRVUZYTEVOQlFVTXNVVUZCVVR0WlFVTTVRaXhoUVVGaExFVkJRVVVzVjBGQlZ5eERRVUZETEdGQlFXRTdXVUZEZUVNc1UwRkJVeXhGUVVGRkxGZEJRVmNzUTBGQlF5eFRRVUZUTzFsQlEyaERMRTlCUVU4c1JVRkJSU3hQUVVGUExFVkJRVVU3VTBGRGJrSXNRMEZCUXp0UlFVVkdMRWxCUVVrN1dVRkRSaXhKUVVGSkxFTkJRVU1zVlVGQlZTeEZRVUZGTEVkQlFVY3NRMEZCUXl4RFFVRkRPMU5CUTNaQ08xRkJRVU1zVDBGQlR5eExRVUZMTEVWQlFVVTdXVUZEWkN4UFFVRlBMRU5CUVVNc1IwRkJSeXhEUVVGRExHdERRVUZyUXl4RFFVRkRMRU5CUVVNN1dVRkRhRVFzYVVKQlFXbENMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU03VTBGRE1VSTdVVUZGUkN4TFFVRkxMRWRCUVVjc1MwRkJTeXhEUVVGRE8wbEJRMmhDTEVOQlFVTTdTVUZGUkN4blFrRkJaMEk3U1VGRGFFSXNVMEZCVXl4UFFVRlBMRU5CUTJRc2QwSkJRV2RETEVWQlEyaERMRWxCUVdkQ0xFVkJRMmhDTEZWQlFXVXNSVUZEWml4WFFVRm5RaXhGUVVOb1FpeFhRVUYzUWp0UlFVVjRRaXhKUVVGSkxFdEJRVXNzUlVGQlJUdFpRVU5VTEU5QlFVODdVMEZEVWp0UlFVTkVMRXRCUVVzc1IwRkJSeXhKUVVGSkxFTkJRVU03VVVGRllpeE5RVUZOTEZOQlFWTXNSMEZCUnl3eVFrRkJNa0lzUTBGRE0wTXNWMEZCVnl4RFFVRkRMRk5CUVZNc1JVRkRja0lzZDBKQlFYZENMRU5CUTNwQ0xFTkJRVU03VVVGRFJpeEpRVUZKTEZOQlFWTXNSVUZCUlR0WlFVTmlMRXRCUVVzc1IwRkJSeXhMUVVGTExFTkJRVU03V1VGRFpDeFBRVUZQTzFOQlExSTdVVUZGUkN4SlFVRkpPMWxCUTBZc2NVVkJRWEZGTzFsQlEzSkZMRTFCUVUwc1ZVRkJWU3hIUVVGaExFVkJRVVVzUTBGQlF6dFpRVU5vUXl4TFFVRkxMRTFCUVUwc1IwRkJSeXhKUVVGSkxFbEJRVWtzUlVGQlJUdG5Ra0ZEZEVJc1ZVRkJWU3hEUVVGRExFbEJRVWtzUTBGRFlpeGxRVUZsTEVOQlFVTXNSMEZCUnl4RlFVRkZMRmRCUVZjc1EwRkJReXh4UWtGQmNVSXNRMEZCUXl4RFFVTjRSQ3hEUVVGRE8yRkJRMGc3V1VGRlJDeEpRVUZKTEhkQ1FVRjNRaXhMUVVGTExDdENRVUVyUWl4RlFVRkZPMmRDUVVNNVJDeEpRVUZKTEZOQlFWTXNSMEZCUnl4TlFVRk5MRU5CUVVNc1RVRkJUU3hEUVVGRExFVkJRVVVzUlVGQlJTeExRVUZMTEVOQlFVTXNTVUZCU1N4RFFVRkRMRlZCUVZVc1JVRkJSU3hEUVVGRExFVkJRVU1zU1VGQlNTeEZRVUZGTEV0QlFVc3NSVUZCUXl4RlFVRkZMRVZCUVVVc1EwRkJReXhEUVVGRExFVkJRVU1zUTBGQlF5eEpRVUZKTEVOQlFVTXNSVUZCUlN4TFFVRkxMRVZCUVVNc1EwRkJReXhEUVVGRExFTkJRVU1zUTBGQlF6dG5Ra0ZEYUVjc1ZVRkJWU3hIUVVGSExGTkJRVk1zUTBGQlF6dGhRVU14UWp0WlFVVkVMRTFCUVUwc1IwRkJSeXhIUVVGSE8yZENRVU5XTEZOQlFWTXNSVUZCUlN4WFFVRlhMRU5CUVVNc1NVRkJTVHRuUWtGRE0wSXNUVUZCVFN4RlFVRkZMSGRDUVVGM1FqdG5Ra0ZEYUVNc1NVRkJTU3hGUVVGRkxGVkJRVlU3WjBKQlEyaENMRXRCUVVzc1JVRkJSU3hGUVVGRk8yZENRVU5VTEZWQlFWVXNSVUZCUlN4bFFVRmxMRU5CUVVNc1ZVRkJWU3hEUVVGRE8yZENRVU4yUXl4VFFVRlRMRVZCUVVVc1YwRkJWeXhEUVVGRExGTkJRVk03WjBKQlEyaERMRlZCUVZVc1JVRkJSU3hYUVVGWExFTkJRVU1zVlVGQlZUdG5Ra0ZEYkVNc1UwRkJVeXhGUVVGRkxGZEJRVmNzUTBGQlF5eFRRVUZUTzJkQ1FVTm9ReXhSUVVGUkxFVkJRVVVzVjBGQlZ5eERRVUZETEZGQlFWRTdaMEpCUXpsQ0xHRkJRV0VzUlVGQlJTeFhRVUZYTEVOQlFVTXNZVUZCWVR0blFrRkRlRU1zVTBGQlV5eEZRVUZGTEZkQlFWY3NRMEZCUXl4VFFVRlRPMmRDUVVOb1F5eFBRVUZQTEVWQlFVVXNUMEZCVHl4RlFVRkZPMkZCUTI1Q0xFTkJRVU03V1VGRFJpeEpRVUZKTEVOQlFVTXNVMEZCVXl4RlFVRkZMRWRCUVVjc1EwRkJReXhEUVVGRE8xTkJRM1JDTzFGQlFVTXNUMEZCVHl4TFFVRkxMRVZCUVVVN1dVRkRaQ3hQUVVGUExFTkJRVU1zUjBGQlJ5eERRVU5VTEd0RFFVRnJReXhIUVVGSExIZENRVUYzUWl4RFFVTTVSQ3hEUVVGRE8xbEJRMFlzYVVKQlFXbENMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU03VTBGRE1VSTdVVUZEUkN4TFFVRkxMRWRCUVVjc1MwRkJTeXhEUVVGRE8wbEJRMmhDTEVOQlFVTTdTVUZGUkN4VFFVRlRMR2xDUVVGcFFpeERRVUZETEV0QlFVc3NSVUZCUlN4VlFVRmxMRXRCUVVzN1VVRkRjRVFzVDBGQlR5eERRVUZETEV0QlFVc3NRMEZCUXl4MVFrRkJkVUlzUjBGQlJ5eExRVUZMTEVOQlFVTXNTVUZCU1N4RFFVRkRMRU5CUVVNN1VVRkRjRVFzVDBGQlR5eERRVUZETEV0QlFVc3NRMEZCUXl3d1FrRkJNRUlzUjBGQlJ5eExRVUZMTEVOQlFVTXNUMEZCVHl4RFFVRkRMRU5CUVVNN1VVRkRNVVFzVDBGQlR5eERRVUZETEV0QlFVc3NRMEZCUXl3eVFrRkJNa0lzUjBGQlJ5eExRVUZMTEVOQlFVTXNVVUZCVVN4RFFVRkRMRU5CUVVNN1VVRkROVVFzVDBGQlR5eERRVUZETEV0QlFVc3NRMEZCUXl3NFFrRkJPRUlzUjBGQlJ5eExRVUZMTEVOQlFVTXNWVUZCVlN4RFFVRkRMRU5CUVVNN1VVRkRha1VzVDBGQlR5eERRVUZETEV0QlFVc3NRMEZCUXl4M1FrRkJkMElzUjBGQlJ5eExRVUZMTEVOQlFVTXNTMEZCU3l4RFFVRkRMRU5CUVVNN1VVRkRkRVFzU1VGQlNTeFBRVUZQTEVWQlFVVTdXVUZEV0N4UFFVRlBMRU5CUVVNc1MwRkJTeXhEUVVGRExEQkNRVUV3UWl4SFFVRkhMRWxCUVVrc1EwRkJReXhUUVVGVExFTkJRVU1zVDBGQlR5eERRVUZETEVOQlFVTXNRMEZCUXp0VFFVTnlSVHRKUVVOSUxFTkJRVU03U1VGRlJDeDNRMEZCZDBNN1NVRkRlRU1zVTBGQlV5eGhRVUZoTzFGQlEzQkNMRWxCUVVrc1MwRkJTeXhEUVVGRE8xRkJSVllzU1VGQlNUdFpRVU5HTEUxQlFVMHNTVUZCU1N4TFFVRkxMRVZCUVVVc1EwRkJRenRUUVVOdVFqdFJRVUZETEU5QlFVOHNSMEZCUnl4RlFVRkZPMWxCUTFvc1MwRkJTeXhIUVVGSExFZEJRVWNzUTBGQlF5eExRVUZMTEVOQlFVTTdVMEZEYmtJN1VVRkZSQ3hQUVVGUExFdEJRVXNzUTBGQlF6dEpRVU5tTEVOQlFVTTdTVUZGUkN3d1EwRkJNRU03U1VGRE1VTXNUVUZCVFN4TlFVRk5MRWRCUVVjc1ZVRkJWU3hOUVVGakxFVkJRVVVzUjBGQlJ5eEZRVUZGTEZGQlFWRTdVVUZEY0VRc1RVRkJUU3hMUVVGTExFZEJRVWNzVFVGQlRTeERRVUZETEV0QlFVc3NRMEZCUXl4SFFVRkhMRU5CUVVNc1EwRkJRenRSUVVOb1F5eFBRVUZQTEZGQlFWRTdXVUZEWWl4RFFVRkRMRU5CUVVNc1EwRkJReXhMUVVGTExFTkJRVU1zUzBGQlN5eERRVUZETEVOQlFVTXNSVUZCUlN4RFFVRkRMRkZCUVZFc1EwRkJReXhEUVVGRExFbEJRVWtzUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRMRTFCUVUwc1EwRkJReXhMUVVGTExFTkJRVU1zUzBGQlN5eERRVUZETEVOQlFVTXNVVUZCVVN4RFFVRkRMRU5CUVVNN1dVRkRkRVVzUTBGQlF5eERRVUZETEV0QlFVc3NRMEZCUXp0SlFVTmFMRU5CUVVNc1EwRkJRenRKUVVWR0xGTkJRVk1zTWtKQlFUSkNMRU5CUVVNc1dVRkJXU3hIUVVGSExFdEJRVXM3VVVGRGRrUXNUVUZCVFN4TFFVRkxMRWRCUVVjc1lVRkJZU3hGUVVGRkxFTkJRVU1zU1VGQlNTeEZRVUZGTEVOQlFVTXNTMEZCU3l4RFFVRkRMRWxCUVVrc1EwRkJReXhEUVVGRE8xRkJRMnBFTEc5RVFVRnZSRHRSUVVOd1JDeE5RVUZOTEdGQlFXRXNSMEZCUnp0WlFVTndRaXhUUVVGVExFVkJRVVVzUlVGQlJUdFpRVU5pTEZWQlFWVXNSVUZCUlN4RlFVRkZPMWxCUTJRc1UwRkJVeXhGUVVGRkxFVkJRVVU3V1VGRFlpeFJRVUZSTEVWQlFVVXNSVUZCUlR0WlFVTmFMR0ZCUVdFc1JVRkJSU3hGUVVGRk8xbEJRMnBDTEZOQlFWTXNSVUZCUlN4RlFVRkZPMU5CUTJRc1EwRkJRenRSUVVOR0xFbEJRVWtzUzBGQlN5eERRVUZETEUxQlFVMHNSMEZCUnl4RFFVRkRMRVZCUVVVN1dVRkRjRUlzVDBGQlR5eGhRVUZoTEVOQlFVTTdVMEZEZEVJN1VVRkRSQ3d3UlVGQk1FVTdVVUZETVVVc1RVRkJUU3hSUVVGUkxFZEJRVWNzUzBGQlN5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRPMUZCUXpGQ0xFbEJRVWtzUTBGQlF5eFJRVUZSTEVWQlFVVTdXVUZEWWl4UFFVRlBMR0ZCUVdFc1EwRkJRenRUUVVOMFFqdFJRVU5FT3pzN096czdPenRYUVZGSE8xRkJRMGdzU1VGQlNUdFpRVU5HTEVsQlFVa3NVMEZCVXl4SFFVRkhMRVZCUVVVc1EwRkJRenRaUVVOdVFpeEpRVUZKTEdGQlFXRXNSMEZCUnl4RlFVRkZMRU5CUVVNc1EwRkJReXcyUWtGQk5rSTdXVUZEY2tRc1RVRkJUU3hoUVVGaExFZEJRVWNzVVVGQlVTeERRVUZETEV0QlFVc3NRMEZCUXl4SFFVRkhMRU5CUVVNc1EwRkJRenRaUVVNeFF5eE5RVUZOTEZGQlFWRXNSMEZCUnl4aFFVRmhMRU5CUVVNc1EwRkJReXhEUVVGRExFbEJRVWtzUlVGQlJTeERRVUZETzFsQlEzaERMRTFCUVUwc1MwRkJTeXhIUVVGSExFMUJRVTBzUTBGQlF5eGhRVUZoTEVOQlFVTXNRMEZCUXl4RFFVRkRMRVZCUVVVc1IwRkJSeXhGUVVGRkxFTkJRVU1zUTBGQlF5eERRVUZETzFsQlF5OURMRTFCUVUwc1VVRkJVU3hIUVVGSExFdEJRVXNzUTBGQlF5eExRVUZMTEVOQlFVTXNUVUZCVFN4SFFVRkhMRU5CUVVNc1EwRkJReXhEUVVGRE8xbEJRM3BETEUxQlFVMHNUVUZCVFN4SFFVRkhMRXRCUVVzc1EwRkJReXhMUVVGTExFTkJRVU1zVFVGQlRTeEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRPMWxCUTNaRExFMUJRVTBzWTBGQll5eEhRVUZITEV0QlFVc3NRMEZCUXl4TFFVRkxMRU5CUVVNc1RVRkJUU3hIUVVGSExFTkJRVU1zUTBGQlF5eEpRVUZKTEVWQlFVVXNRMEZCUXp0WlFVTnlSQ3hOUVVGTkxGTkJRVk1zUjBGQlJ5eGpRVUZqTEVOQlFVTXNUMEZCVHl4RFFVRkRMRkZCUVZFc1EwRkJReXhEUVVGRExFTkJRVU1zZVVOQlFYbERPMWxCUXpkR0xFbEJRVWtzVTBGQlV5eExRVUZMTEVOQlFVTXNRMEZCUXl4RlFVRkZPMmRDUVVOd1FpeFRRVUZUTEVkQlFVY3NZMEZCWXl4RFFVRkRMRU5CUVVNc2IwUkJRVzlFTzJGQlEycEdPMmxDUVVGTk8yZENRVU5NTEZOQlFWTXNSMEZCUnl4alFVRmpMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU1zUlVGQlJTeFRRVUZUTEVOQlFVTXNRMEZCUXp0blFrRkRMME1zWVVGQllTeEhRVUZITEdOQlFXTXNRMEZCUXl4TFFVRkxMRU5CUTJ4RExGTkJRVk1zUjBGQlJ5eERRVUZETEVWQlEySXNZMEZCWXl4RFFVRkRMRTFCUVUwc1EwRkRkRUlzUTBGQlF6dGhRVU5JTzFsQlEwUXNUVUZCVFN4WFFVRlhMRWRCUVVjN1owSkJRMnhDTEZOQlFWTTdaMEpCUTFRc1ZVRkJWU3hGUVVGRkxFMUJRVTA3WjBKQlEyeENMRk5CUVZNc1JVRkJSU3hSUVVGUk8yZENRVU51UWl4UlFVRlJPMmRDUVVOU0xHRkJRV0U3WjBKQlEySXNVMEZCVXl4RlFVRkZMRmxCUVZrc1EwRkJReXhEUVVGRExFTkJRVU1zUzBGQlN5eERRVUZETEV0QlFVc3NRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhKUVVGSkxFTkJRVU1zU1VGQlNTeERRVUZETEVOQlFVTXNTVUZCU1N4RlFVRkZMRU5CUVVNc1EwRkJReXhEUVVGRExFVkJRVVU3WVVGRGFFVXNRMEZCUXp0WlFVTkdMRTlCUVU4c1YwRkJWeXhEUVVGRE8xTkJRM0JDTzFGQlFVTXNUMEZCVHl4RFFVRkRMRVZCUVVVN1dVRkRWaXhQUVVGUExFTkJRVU1zUjBGQlJ5eERRVU5VTERKRFFVRXlReXhGUVVNelF5eERRVUZETEVOQlFVTXNVVUZCVVN4RlFVRkZMRVZCUTFvc1VVRkJVU3hEUVVOVUxFTkJRVU03V1VGRFJpeFBRVUZQTEdGQlFXRXNRMEZCUXp0VFFVTjBRanRKUVVOSUxFTkJRVU03U1VGRlJDeFRRVUZUTEZGQlFWRXNRMEZCUXl4TlFVRk5MRVZCUVVVc1dVRkJXVHRSUVVOd1F5eEpRVUZKTEZGQlFWRXNRMEZCUXp0UlFVTmlMRWxCUVVrN1dVRkRSaXhSUVVGUkxFZEJRVWNzVFVGQlRTeERRVUZETEZsQlFWa3NRMEZCUXl4RFFVRkRPMU5CUTJwRE8xRkJRVU1zVDBGQlR5eExRVUZMTEVWQlFVVTdXVUZEWkN4UFFVRlBMRXRCUVVzc1EwRkJRenRUUVVOa08xRkJRMFFzU1VGQlNTeFJRVUZSTEV0QlFVc3NTVUZCU1N4RlFVRkZPMWxCUTNKQ0xIZENRVUYzUWp0WlFVTjRRaXhQUVVGUExFdEJRVXNzUTBGQlF6dFRRVU5rTzFGQlEwUXNUMEZCVHl4UFFVRlBMRkZCUVZFc1MwRkJTeXhSUVVGUkxFTkJRVU03U1VGRGRFTXNRMEZCUXp0SlFVVkVMR2REUVVGblF6dEpRVU5vUXl4M1JVRkJkMFU3U1VGRGVFVXNlVVZCUVhsRk8wbEJRM3BGTEhkRVFVRjNSRHRKUVVONFJDeFRRVUZUTEd0Q1FVRnJRaXhEUVVONlFpeFZRVUZyUWl4RlFVTnNRaXhWUVVGclFpeEZRVU5zUWl4SlFVRlRMRVZCUTFRc1YwRkJkMEk3VVVGSGVFSXNUMEZCVHp0WlFVTk1MRTFCUVUwc1YwRkJWeXhIUVVGSExESkNRVUV5UWl4RFFVRkRMRmRCUVZjc1EwRkJReXhaUVVGWkxFTkJRVU1zUTBGQlF6dFpRVVV4UlN4MVFrRkJkVUk3V1VGRGRrSXNTVUZCU1N4VlFVRlZMRWRCUVVjc1JVRkJSU3hEUVVGRE8xbEJSWEJDTERKRlFVRXlSVHRaUVVNelJTeHJRa0ZCYTBJN1dVRkRiRUlzU1VGQlNTeFZRVUZWTEVsQlFVa3NaVUZCWlN4RlFVRkZPMmRDUVVOcVF5eDVSVUZCZVVVN1owSkJRM3BGTEdkRlFVRm5SVHRuUWtGRGFFVXNjME5CUVhORE8yZENRVU4wUXl3MlFrRkJOa0k3WjBKQlF6ZENMSEZEUVVGeFF6dG5Ra0ZEY2tNc1owTkJRV2RETzJkQ1FVTm9ReXgxUTBGQmRVTTdaMEpCUTNaRExDdEdRVUVyUmp0blFrRkRMMFlzWVVGQllUdG5Ra0ZEWWl4cFEwRkJhVU03WjBKQlEycERMRFpDUVVFMlFqdG5Ra0ZETjBJc05FWkJRVFJHTzJkQ1FVTTFSaXhoUVVGaE8yZENRVU5pTEdkRFFVRm5RenRuUWtGRGFFTXNLMEpCUVN0Q08yZENRVU12UWl3NFJrRkJPRVk3WjBKQlF6bEdMR0ZCUVdFN1owSkJRMklzVFVGQlRUdG5Ra0ZEVGl4aFFVRmhPMmRDUVVOaUxFbEJRVWtzVDBGQlR5eEhRVUZITEVsQlFVa3NRMEZCUXl4TFFVRkxMRU5CUVVNc1NVRkJTU3hGUVVGRkxGTkJRVk1zUTBGQlF5eERRVUZETzJkQ1FVTXhReXgxUTBGQmRVTTdaMEpCUTNaRExFbEJRVWtzUjBGQlJ5eEhRVUZITEVsQlFVa3NRMEZCUXl4TFFVRkxMRU5CUVVNc1NVRkJTU3hEUVVGRExFMUJRVTBzUlVGQlJTeEhRVUZITEVsQlFVa3NRMEZCUXl4SFFVRkhMRU5CUVVNc1JVRkJSU3hGUVVGRkxFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTTdaMEpCUTNSRUxFOUJRVThzUTBGQlF5eFpRVUZaTEVOQlFVTXNVMEZCVXl4RlFVRkZMRWRCUVVjc1EwRkJReXhEUVVGRE8yZENRVU55UXl4VlFVRlZMRWRCUVVjc1QwRkJUeXhEUVVGRExGVkJRVlVzUTBGQlF6dG5Ra0ZEYUVNc1QwRkJUeXhEUVVOTUxGVkJRVlVzUjBGQlJ5eEhRVUZITEVkQlFVY3NWVUZCVlN4RlFVTTNRaXhUUVVGVExFVkJRMVFzVlVGQlZTeEZRVU5XTEZkQlFWY3NSVUZEV0N4WFFVRlhMRU5CUTFvc1EwRkJRenRuUWtGRFJpeFBRVUZQTEU5QlFVOHNRMEZCUXp0aFFVTm9RanRaUVVORUxIVkNRVUYxUWp0WlFVVjJRaXhQUVVGUExFTkJRMHdzVlVGQlZTeEhRVUZITEVkQlFVY3NSMEZCUnl4VlFVRlZMRVZCUXpkQ0xGTkJRVk1zUlVGRFZDeFZRVUZWTEVWQlExWXNWMEZCVnl4RlFVTllMRmRCUVZjc1EwRkRXaXhEUVVGRE8xbEJRMFlzVDBGQlR5eEpRVUZKTEVOQlFVTXNTMEZCU3l4RFFVRkRMRWxCUVVrc1JVRkJSU3hUUVVGVExFTkJRVU1zUTBGQlF6dFJRVU55UXl4RFFVRkRMRU5CUVVNN1NVRkRTaXhEUVVGRE8wbEJSVVFzTWtOQlFUSkRPMGxCUXpORExGTkJRVk1zZDBKQlFYZENMRU5CUXk5Q0xFMUJRVTBzUlVGRFRpeFZRVUZyUWl4RlFVTnNRaXhaUVVGdlFpeEZRVU53UWl4WFFVRjNRanRSUVVWNFFpeEpRVU5GTEVOQlFVTXNUVUZCVFR0WlFVTlFMRU5CUVVNc1ZVRkJWVHRaUVVOWUxFTkJRVU1zV1VGQldUdFpRVU5pTEZsQlFWa3NTMEZCU3l4WFFVRlhMRVZCUXpWQ08xbEJRMEVzVFVGQlRTeEpRVUZKTEV0QlFVc3NRMEZEWWp0clFrRkRWU3hOUVVGTk8zTkNRVU5HTEZWQlFWVTdkMEpCUTFJc1dVRkJXVHRUUVVNelFpeERRVU5HTEVOQlFVTTdVMEZEU0R0UlFVVkVMSFZEUVVGMVF6dFJRVU4yUXl4TlFVRk5MRkZCUVZFc1IwRkJSeXhOUVVGTkxFTkJRVU1zY1VKQlFYRkNMRU5CUVVNc1RVRkJUU3hGUVVGRkxGbEJRVmtzUTBGQlF5eERRVUZETzFGQlJYQkZMRzlHUVVGdlJqdFJRVU53Uml4SlFVTkZMRU5CUVVNc1VVRkJVVHRaUVVOVUxFTkJRVU1zVjBGQlZ5eERRVUZETEdsRFFVRnBReXhEUVVGRExGRkJRVkVzUTBGQlF5eFpRVUZaTEVOQlFVTXNSVUZEY2tVN1dVRkRRU3hQUVVGUExFTkJRVU1zUzBGQlN5eERRVU5ZTEcxRFFVRnRReXhGUVVOdVF5eFZRVUZWTEVWQlExWXNXVUZCV1N4RlFVTmFMRTFCUVUwc1EwRkRVQ3hEUVVGRE8xbEJRMFlzVDBGQlR6dFRRVU5TTzFGQlJVUXNLME5CUVN0RE8xRkJReTlETEVsQlFVa3NhMEpCUVd0Q0xFTkJRVU03VVVGRGRrSXNUVUZCVFN4cFFrRkJhVUlzUjBGQlJ6dFpRVU40UWl4SFFVRkhMRVZCUVVVc1IwRkJSeXhGUVVGRk8yZENRVU5TTEU5QlFVOHNhMEpCUVd0Q0xFTkJRVU03V1VGRE5VSXNRMEZCUXp0WlFVTkVMRWRCUVVjc1JVRkJSU3hEUVVGRExFdEJRVXNzUlVGQlJTeEZRVUZGTzJkQ1FVTmlMR3RDUVVGclFpeEhRVUZITEV0QlFVc3NRMEZCUXp0WlFVTTNRaXhEUVVGRE8xbEJRMFFzVlVGQlZTeEZRVUZGTEV0QlFVczdVMEZEYkVJc1EwRkJRenRSUVVWR0xHMUVRVUZ0UkR0UlFVTnVSQ3hOUVVGTkxHTkJRV01zUjBGQlJ5eFJRVUZSTEVOQlFVTXNRMEZCUXl4RFFVRkRMRkZCUVZFc1EwRkJReXhIUVVGSExFTkJRVU1zUTBGQlF5eERRVUZETEdsQ1FVRnBRaXhEUVVGRExFZEJRVWNzUTBGQlF6dFJRVU4yUlN4TlFVRk5MR05CUVdNc1IwRkJSeXhSUVVGUkxFTkJRVU1zUTBGQlF5eERRVUZETEZGQlFWRXNRMEZCUXl4SFFVRkhMRU5CUVVNc1EwRkJReXhEUVVGRExHbENRVUZwUWl4RFFVRkRMRWRCUVVjc1EwRkJRenRSUVVOMlJTeEpRVUZKTEdGQlFXRXNSMEZCUnl4UlFVRlJMRU5CUVVNc1EwRkJReXhEUVVGRExGRkJRVkVzUTBGQlF5eExRVUZMTEVOQlFVTXNRMEZCUXl4RFFVRkRMR3RDUVVGclFpeERRVUZETzFGQlJXNUZMRzlGUVVGdlJUdFJRVU53UlN4dlFrRkJiMEk3VVVGRGNFSXNUVUZCVFN4RFFVRkRMR05CUVdNc1EwRkJReXhOUVVGTkxFVkJRVVVzV1VGQldTeEZRVUZGTzFsQlF6RkRMRmxCUVZrc1JVRkJSU3hKUVVGSk8xbEJRMnhDTEVkQlFVY3NSVUZCUlN4RFFVRkRPMmRDUVVOS0xFOUJRVTg3YjBKQlEwd3NTVUZCU1N4WlFVRlpMRU5CUVVNN2IwSkJRMnBDTEUxQlFVMHNWMEZCVnl4SFFVRkhMREpDUVVFeVFpeERRVU0zUXl4WFFVRlhMRU5CUVVNc1dVRkJXU3hEUVVONlFpeERRVUZETzI5Q1FVTkdMRTFCUVUwc2QwSkJRWGRDTEVkQlFVY3NSMEZCUnl4VlFVRlZMRWxCUVVrc1dVRkJXU3hGUVVGRkxFTkJRVU03YjBKQlEycEZMRWxCUVVrc1ZVRkJWU3hIUVVGSExFVkJRVVVzUTBGQlF6dHZRa0ZGY0VJc2NVSkJRWEZDTzI5Q1FVTnlRaXhKUVVGSkxFTkJRVU1zVVVGQlVTeEZRVUZGTzNkQ1FVTmlMSGRDUVVGM1FqdDNRa0ZEZUVJc1dVRkJXU3hIUVVGSExHdENRVUZyUWl4RFFVRkRPM0ZDUVVOdVF6dDVRa0ZCVFN4SlFVRkpMR05CUVdNc1JVRkJSVHQzUWtGRGVrSXNkVUpCUVhWQ08zZENRVU4yUWl4WlFVRlpMRWRCUVVjc1kwRkJZeXhEUVVGRExFbEJRVWtzUTBGQlF5eEpRVUZKTEVOQlFVTXNRMEZCUXp0eFFrRkRNVU03ZVVKQlFVMHNTVUZCU1N4UFFVRlBMRWxCUVVrc1VVRkJVU3hGUVVGRk8zZENRVU01UWl4dFFrRkJiVUk3ZDBKQlEyNUNMRmxCUVZrc1IwRkJSeXhoUVVGaExFTkJRVU03Y1VKQlF6bENPM2xDUVVGTk8zZENRVU5NTEU5QlFVOHNRMEZCUXl4TFFVRkxMRU5CUTFnc01rSkJRVEpDTEhkQ1FVRjNRaXhuUTBGQlowTXNRMEZEY0VZc1EwRkJRenQzUWtGRFJpeFJRVUZSTEVOQlEwNHNkMEpCUVhkQ0xFVkJRM2hDTEVWQlFVVXNSVUZEUml4VlFVRlZMRVZCUTFZc1YwRkJWeXhEUVVGRExGVkJRVlVzUlVGRGRFSXNWMEZCVnl4RlFVTllMRmRCUVZjc1EwRkRXaXhEUVVGRE8zZENRVU5HTEU5QlFVODdjVUpCUTFJN2IwSkJSVVFzSzBSQlFTdEVPMjlDUVVNdlJDd3lSRUZCTWtRN2IwSkJRek5FTEhORVFVRnpSRHR2UWtGRGRFUXNhMFZCUVd0Rk8yOUNRVU5zUlN4SlFVRkpMRTlCUVU4c1dVRkJXU3hMUVVGTExGVkJRVlVzUlVGQlJUdDNRa0ZEZEVNc1NVRkJTU3hYUVVGWExFTkJRVU1zWlVGQlpTeEZRVUZGT3pSQ1FVTXZRaXhSUVVGUkxFTkJRMDRzZDBKQlFYZENMRVZCUTNoQ0xGbEJRVmtzUlVGRFdpeFZRVUZWTEVWQlExWXNWMEZCVnl4RFFVRkRMRmxCUVZrc1JVRkRlRUlzVjBGQlZ5eEZRVU5ZTEZkQlFWY3NRMEZEV2l4RFFVRkRPM2xDUVVOSU8zZENRVU5FTEUxQlFVMHNNa0pCUVRKQ0xFZEJRVWNzYTBKQlFXdENMRU5CUTNCRUxGVkJRVlVzUlVGRFZpeFpRVUZaTEVWQlExb3NXVUZCV1N4RlFVTmFMRmRCUVZjc1EwRkRXaXhEUVVGRE8zZENRVU5HTERSR1FVRTBSanQzUWtGRE5VWXNNRWRCUVRCSE8zZENRVU14Unl4SlFVRkpMRmxCUVZrc1EwRkJReXhUUVVGVExFVkJRVVU3TkVKQlF6RkNMREpDUVVFeVFpeERRVUZETEZOQlFWTXNSMEZCUnl4WlFVRlpMRU5CUVVNc1UwRkJVeXhEUVVGRE96UkNRVU12UkN4SlFVRkpMRmxCUVZrc1EwRkJReXhUUVVGVExFTkJRVU1zVjBGQlZ5eEZRVUZGTzJkRFFVTjBReXd5UWtGQk1rSXNRMEZCUXl4VFFVRlRMRU5CUVVNc1YwRkJWenR2UTBGREwwTXNXVUZCV1N4RFFVRkRMRk5CUVZNc1EwRkJReXhYUVVGWExFTkJRVU03TmtKQlEzUkRPM2xDUVVOR08zZENRVU5FTEU5QlFVOHNNa0pCUVRKQ0xFTkJRVU03Y1VKQlEzQkRPM2xDUVVGTkxFbEJRMHdzVDBGQlR5eFpRVUZaTEV0QlFVc3NVVUZCVVR0M1FrRkRhRU1zVjBGQlZ5eERRVUZETEZOQlFWTTdkMEpCUTNKQ0xGZEJRVmNzUTBGQlF5eExRVUZMTEVkQlFVY3NRMEZCUXl4RlFVTnlRanQzUWtGRFFTeFBRVUZQTEZsQlFWa3NRMEZCUXp0eFFrRkRja0k3ZVVKQlFVMDdkMEpCUTB3c1VVRkJVU3hEUVVOT0xIZENRVUYzUWl4RlFVTjRRaXhaUVVGWkxFVkJRMW9zVlVGQlZTeEZRVU5XTEZkQlFWY3NRMEZCUXl4SFFVRkhMRVZCUTJZc1YwRkJWeXhGUVVOWUxGZEJRVmNzUTBGRFdpeERRVUZETzNkQ1FVTkdMRTlCUVU4c1dVRkJXU3hEUVVGRE8zRkNRVU55UWp0blFrRkRTQ3hEUVVGRExFTkJRVU03V1VGRFNpeERRVUZETEVOQlFVTXNSVUZCUlR0WlFVTktMRWRCUVVjc1JVRkJSU3hEUVVGRE8yZENRVU5LTEU5QlFVOHNWVUZCVlN4TFFVRkxPMjlDUVVOd1FpeE5RVUZOTEZkQlFWY3NSMEZCUnl3eVFrRkJNa0lzUTBGRE4wTXNWMEZCVnl4RFFVRkRMRmxCUVZrc1EwRkRla0lzUTBGQlF6dHZRa0ZEUml4TlFVRk5MSGRDUVVGM1FpeEhRVUZITEVkQlFVY3NWVUZCVlN4SlFVRkpMRmxCUVZrc1JVRkJSU3hEUVVGRE8yOUNRVU5xUlN4SlFVRkpMRmRCUVZjc1EwRkJRenR2UWtGRGFFSXNTVUZCU1N4VlFVRlZMRWRCUVVjc1JVRkJSU3hEUVVGRE8yOUNRVVZ3UWl4dlJFRkJiMFE3YjBKQlEzQkVMRWxCUTBVc1YwRkJWeXhEUVVGRExGZEJRVmM3ZDBKQlEzWkNMRU5CUVVNc1QwRkJUeXhoUVVGaExFdEJRVXNzVlVGQlZUczBRa0ZEYkVNc1QwRkJUeXhoUVVGaExFdEJRVXNzVVVGQlVTeERRVUZETEVWQlEzQkRPM2RDUVVOQkxGRkJRVkVzUTBGRFRpeDNRa0ZCZDBJc1JVRkRlRUlzUzBGQlN5eEZRVU5NTEZWQlFWVXNSVUZEVml4WFFVRlhMRU5CUVVNc1lVRkJZU3hGUVVONlFpeFhRVUZYTEVWQlExZ3NWMEZCVnl4RFFVTmFMRU5CUVVNN2QwSkJRMFlzVDBGQlR5eExRVUZMTEVOQlFVTTdjVUpCUTJRN2IwSkJSVVFzTkVOQlFUUkRPMjlDUVVNMVF5eEpRVUZKTEdOQlFXTXNSVUZCUlR0M1FrRkRiRUlzZFVKQlFYVkNPM2RDUVVOMlFpeFhRVUZYTEVkQlFVY3NZMEZCWXl4RFFVRkRMRWxCUVVrc1EwRkJReXhKUVVGSkxFVkJRVVVzUzBGQlN5eERRVUZETEVOQlFVTTdkMEpCUXk5RExFbEJRVWs3TkVKQlEwWXNTVUZCU1N4SlFVRkpMRU5CUVVNc1dVRkJXU3hEUVVGRExGTkJRVk1zUTBGQlF5eEZRVUZGTzJkRFFVTTVRaXhWUVVGVkxFZEJRVWNzU1VGQlNTeERRVUZETEZWQlFWVXNRMEZCUXpzMlFrRkRhRU03ZVVKQlEwWTdkMEpCUVVNc1QwRkJUeXhMUVVGTExFVkJRVVU3TkVKQlEyUXNUMEZCVHl4RFFVRkRMRWxCUVVrc1EwRkJReXh2UTBGQmIwTXNRMEZCUXl4RFFVRkRPelJDUVVOdVJDeFZRVUZWTEVkQlFVY3NSVUZCUlN4RFFVRkRPM2xDUVVOcVFqdHhRa0ZEUmp0NVFrRkJUU3hKUVVGSkxFOUJRVThzU1VGQlNTeFJRVUZSTEVWQlFVVTdkMEpCUXpsQ0xFdEJRVXNzUjBGQlJ5eEpRVUZKTEVOQlFVTTdkMEpCUTJJc1NVRkJTU3hOUVVGTkxFTkJRVU1zWVVGQllTeERRVUZETEVsQlFVa3NRMEZCUXl4RlFVRkZPelJDUVVNNVFpeE5RVUZOTEVOQlFVTXNZMEZCWXl4RFFVRkRMRWxCUVVrc1JVRkJSU3haUVVGWkxFVkJRVVU3WjBOQlEzaERMRXRCUVVzN05rSkJRMDRzUTBGQlF5eERRVUZETzNsQ1FVTktPelpDUVVGTk96UkNRVU5NTEdGQlFXRXNSMEZCUnl4TFFVRkxMRU5CUVVNN2VVSkJRM1pDTzNkQ1FVTkVMRmRCUVZjc1IwRkJSeXhMUVVGTExFTkJRVU03ZDBKQlEzQkNMRXRCUVVzc1IwRkJSeXhMUVVGTExFTkJRVU03Y1VKQlEyWTdlVUpCUVUwN2QwSkJRMHdzVDBGQlR5eERRVUZETEV0QlFVc3NRMEZEV0N3eVFrRkJNa0lzZDBKQlFYZENMR2REUVVGblF5eERRVU53Uml4RFFVRkRPM2RDUVVOR0xGRkJRVkVzUTBGRFRpeDNRa0ZCZDBJc1JVRkRlRUlzUzBGQlN5eEZRVU5NTEZWQlFWVXNSVUZEVml4WFFVRlhMRU5CUVVNc1ZVRkJWU3hGUVVOMFFpeFhRVUZYTEVWQlExZ3NWMEZCVnl4RFFVTmFMRU5CUVVNN2QwSkJRMFlzVDBGQlR5eExRVUZMTEVOQlFVTTdjVUpCUTJRN2IwSkJRMFFzVVVGQlVTeERRVU5PTEhkQ1FVRjNRaXhGUVVONFFpeExRVUZMTEVWQlEwd3NWVUZCVlN4RlFVTldMRmRCUVZjc1EwRkJReXhIUVVGSExFVkJRMllzVjBGQlZ5eEZRVU5ZTEZkQlFWY3NRMEZEV2l4RFFVRkRPMjlDUVVOR0xFOUJRVThzVjBGQlZ5eERRVUZETzJkQ1FVTnlRaXhEUVVGRExFTkJRVU03V1VGRFNpeERRVUZETEVOQlFVTXNSVUZCUlR0VFFVTk1MRU5CUVVNc1EwRkJRenRKUVVOTUxFTkJRVU03U1VGRlJDeFRRVUZUTEdkQ1FVRm5RaXhEUVVOMlFpeE5RVUZYTEVWQlExZ3NaMEpCUVhkQ0xFVkJRM2hDTEZkQlFYZENPMUZCUlhoQ0xHZEdRVUZuUmp0UlFVTm9SaXgzUTBGQmQwTTdVVUZEZUVNc1NVRkJTU3h6UWtGQlowTXNRMEZCUXp0UlFVTnlReXhKUVVGSkxGZEJRVmNzUTBGQlF5eHpRa0ZCYzBJc1MwRkJTeXhKUVVGSkxFVkJRVVU3V1VGREwwTXNjMEpCUVhOQ0xFZEJRVWNzUlVGQlJTeERRVUZETzFOQlF6ZENPMkZCUVUwc1NVRkJTU3hYUVVGWExFTkJRVU1zYzBKQlFYTkNMRU5CUVVNc1RVRkJUU3hMUVVGTExFTkJRVU1zUlVGQlJUdFpRVU14UkN4elFrRkJjMElzUjBGQlJ5eE5RVUZOTEVOQlFVTXNaMEpCUVdkQ0xFTkJRVU1zVFVGQlRTeERRVUZETEVOQlFVTTdVMEZETVVRN1lVRkJUVHRaUVVOTUxITkNRVUZ6UWl4SFFVRkhMRmRCUVZjc1EwRkJReXh6UWtGQmMwSXNRMEZCUXp0VFFVTTNSRHRSUVVORUxFdEJRVXNzVFVGQlRTeFpRVUZaTEVsQlFVa3NjMEpCUVhOQ0xFVkJRVVU3V1VGRGFrUXNTVUZCU1N4WFFVRlhMRU5CUVVNc2EwSkJRV3RDTEVOQlFVTXNVVUZCVVN4RFFVRkRMRmxCUVZrc1EwRkJReXhGUVVGRk8yZENRVU42UkN4VFFVRlRPMkZCUTFZN1dVRkRSQ3huUlVGQlowVTdXVUZEYUVVc2MwUkJRWE5FTzFsQlEzUkVMRWxCUTBVc1YwRkJWeXhEUVVGRExGTkJRVk03WjBKQlEzSkNMRmRCUVZjc1EwRkJReXhMUVVGTExFZEJRVWNzUTBGQlF6dG5Ra0ZEY2tJc1VVRkJVU3hEUVVGRExFMUJRVTBzUlVGQlJTeFpRVUZaTEVOQlFVTTdaMEpCUXpsQ0xGbEJRVmtzUzBGQlN5eFhRVUZYTEVWQlF6VkNPMmRDUVVOQkxFMUJRVTBzYlVKQlFXMUNMRWRCUVVjc1IwRkJSeXhuUWtGQlowSXNTVUZCU1N4WlFVRlpMRVZCUVVVc1EwRkJRenRuUWtGRGJFVXNUVUZCVFN4alFVRmpMRWRCUVVjc1JVRkJSU3hIUVVGSExGZEJRVmNzUlVGQlJTeERRVUZETzJkQ1FVTXhReXhqUVVGakxFTkJRVU1zUzBGQlN5eEhRVUZITEZkQlFWY3NRMEZCUXl4TFFVRkxMRWRCUVVjc1EwRkJReXhEUVVGRE8yZENRVU0zUXl4alFVRmpMRU5CUVVNc2MwSkJRWE5DTEVkQlFVY3NSVUZCUlN4RFFVRkRPMmRDUVVNelF5eG5Ra0ZCWjBJc1EwRkRaQ3hOUVVGTkxFTkJRVU1zV1VGQldTeERRVUZETEVWQlEzQkNMRzFDUVVGdFFpeEZRVU51UWl4alFVRmpMRU5CUTJZc1EwRkJRenRoUVVOSU8xbEJRMFFzU1VGQlNUdG5Ra0ZEUml4M1FrRkJkMElzUTBGRGRFSXNUVUZCVFN4RlFVTk9MR2RDUVVGblFpeEZRVU5vUWl4WlFVRlpMRVZCUTFvc1YwRkJWeXhEUVVOYUxFTkJRVU03WVVGRFNEdFpRVUZETEU5QlFVOHNTMEZCU3l4RlFVRkZPMmRDUVVOa0xFbEJRMFVzUzBGQlN5eFpRVUZaTEZOQlFWTTdiMEpCUXpGQ0xFdEJRVXNzUTBGQlF5eFBRVUZQTEVOQlFVTXNVVUZCVVN4RFFVRkRMREJEUVVFd1F5eERRVUZETEVWQlEyeEZPMjlDUVVOQkxFOUJRVThzUTBGQlF5eEpRVUZKTEVOQlExWXNaMFJCUVdkRUxHZENRVUZuUWl4SlFVRkpMRmxCUVZrc1JVRkJSU3hEUVVOdVJpeERRVUZETzJsQ1FVTklPM0ZDUVVGTk8yOUNRVU5NTEdsQ1FVRnBRaXhEUVVGRExFdEJRVXNzUlVGQlJTeEZRVUZGTEdkQ1FVRm5RaXhGUVVGRkxGbEJRVmtzUlVGQlJTeERRVUZETEVOQlFVTTdhVUpCUXpsRU8yRkJRMFk3VTBGRFJqdFJRVU5FTEV0QlFVc3NUVUZCVFN4WlFVRlpMRWxCUVVrc1YwRkJWeXhEUVVGRExHbERRVUZwUXl4RlFVRkZPMWxCUTNoRkxFbEJRVWtzVjBGQlZ5eERRVUZETEd0Q1FVRnJRaXhEUVVGRExGRkJRVkVzUTBGQlF5eFpRVUZaTEVOQlFVTXNSVUZCUlR0blFrRkRla1FzVTBGQlV6dGhRVU5XTzFsQlEwUXNTVUZCU1R0blFrRkRSaXgzUWtGQmQwSXNRMEZEZEVJc1RVRkJUU3hGUVVOT0xHZENRVUZuUWl4RlFVTm9RaXhaUVVGWkxFVkJRMW9zVjBGQlZ5eERRVU5hTEVOQlFVTTdZVUZEU0R0WlFVRkRMRTlCUVU4c1MwRkJTeXhGUVVGRk8yZENRVU5rTEdsQ1FVRnBRaXhEUVVGRExFdEJRVXNzUlVGQlJTeEZRVUZGTEdkQ1FVRm5RaXhGUVVGRkxGbEJRVmtzUlVGQlJTeERRVUZETEVOQlFVTTdZVUZET1VRN1UwRkRSanRKUVVOSUxFTkJRVU03U1VGRlJDeE5RVUZOTEZkQlFWY3NSMEZCUnl4VlFVRlZMRTlCUVU4c1JVRkJSU3h4UWtGQmNVSTdVVUZETVVRc1NVRkJTU3hSUVVGUkxFZEJRVWNzUlVGQlJTeERRVUZETzFGQlEyeENMRzFEUVVGdFF6dFJRVU51UXl4TlFVRk5MRXRCUVVzc1IwRkJSeXhSUVVGUkxFTkJRVU03V1VGRGNrSXNjVUpCUVhGQ0xFTkJRVU1zVDBGQlR5eEZRVUZGTEZGQlFWRXNRMEZCUXl4RFFVRkRPMWxCUlhwRExHdENRVUZyUWp0WlFVTnNRaXhSUVVGUkxFZEJRVWNzUlVGQlJTeERRVUZETzFGQlEyaENMRU5CUVVNc1JVRkJSU3hIUVVGSExFTkJRVU1zUTBGQlF6dFJRVVZTTEU5QlFVOHNWVUZCVlN4UFFVRlBMRVZCUVVVc1IwRkJSenRaUVVNelFpeHZRa0ZCYjBJN1dVRkRjRUlzVVVGQlVTeERRVUZETEVsQlFVa3NRMEZCUXl4RlFVRkZMRWxCUVVrc1JVRkJSU3hQUVVGUExFVkJRVVVzVDBGQlR5eEZRVUZGTEVkQlFVY3NSVUZCUlN4RFFVRkRMRU5CUVVNN1dVRkRMME1zUzBGQlN5eEZRVUZGTEVOQlFVTTdVVUZEVml4RFFVRkRMRU5CUVVNN1NVRkRTaXhEUVVGRExFTkJRVU03U1VGRlJpeE5RVUZOTEVsQlFVa3NSMEZCUnl4WFFVRlhMRU5CUVVNc1QwRkJUeXhGUVVGRkxHOUNRVUZ2UWl4RFFVRkRMRU5CUVVNN1NVRkZlRVFzVTBGQlV5eFpRVUZaTEVOQlFVTXNiMEpCUVRKRE8xRkJReTlFTEdsRlFVRnBSVHRSUVVOcVJTdzRRMEZCT0VNN1VVRkZPVU1zZVVSQlFYbEVPMUZCUTNwRUxHbEVRVUZwUkR0UlFVTnFSQ3h2UWtGQmIwSXNRMEZCUXl4UFFVRlBMRU5CUVVNc1ZVRkJWU3hKUVVGSk8xbEJRM3BETEdkQ1FVRm5RaXhEUVVOa0xFbEJRVWtzUTBGQlF5eEpRVUZKTEVOQlFVTXNUVUZCVFN4RFFVRkRMRVZCUTJwQ0xFbEJRVWtzUTBGQlF5eG5Ra0ZCWjBJc1JVRkRja0lzU1VGQlNTeERRVUZETEZkQlFWY3NRMEZEYWtJc1EwRkJRenRSUVVOS0xFTkJRVU1zUTBGQlF5eERRVUZETzBsQlEwd3NRMEZCUXp0SlFVVkVMR2RHUVVGblJqdEpRVU5vUml4UFFVRlBMRmxCUVZrc1EwRkJRenRCUVVOMFFpeERRVUZESW4wPSIsIi8qKlxuICogVGllcyB0b2dldGhlciB0aGUgdHdvIHNlcGFyYXRlIG5hdmlnYXRpb24gZXZlbnRzIHRoYXQgdG9nZXRoZXIgaG9sZHMgaW5mb3JtYXRpb24gYWJvdXQgYm90aCBwYXJlbnQgZnJhbWUgaWQgYW5kIHRyYW5zaXRpb24tcmVsYXRlZCBhdHRyaWJ1dGVzXG4gKi9cbmV4cG9ydCBjbGFzcyBQZW5kaW5nTmF2aWdhdGlvbiB7XG4gICAgb25CZWZvcmVOYXZpZ2F0ZUV2ZW50TmF2aWdhdGlvbjtcbiAgICBvbkNvbW1pdHRlZEV2ZW50TmF2aWdhdGlvbjtcbiAgICByZXNvbHZlT25CZWZvcmVOYXZpZ2F0ZUV2ZW50TmF2aWdhdGlvbjtcbiAgICByZXNvbHZlT25Db21taXR0ZWRFdmVudE5hdmlnYXRpb247XG4gICAgY29uc3RydWN0b3IoKSB7XG4gICAgICAgIHRoaXMub25CZWZvcmVOYXZpZ2F0ZUV2ZW50TmF2aWdhdGlvbiA9IG5ldyBQcm9taXNlKChyZXNvbHZlKSA9PiB7XG4gICAgICAgICAgICB0aGlzLnJlc29sdmVPbkJlZm9yZU5hdmlnYXRlRXZlbnROYXZpZ2F0aW9uID0gcmVzb2x2ZTtcbiAgICAgICAgfSk7XG4gICAgICAgIHRoaXMub25Db21taXR0ZWRFdmVudE5hdmlnYXRpb24gPSBuZXcgUHJvbWlzZSgocmVzb2x2ZSkgPT4ge1xuICAgICAgICAgICAgdGhpcy5yZXNvbHZlT25Db21taXR0ZWRFdmVudE5hdmlnYXRpb24gPSByZXNvbHZlO1xuICAgICAgICB9KTtcbiAgICB9XG4gICAgcmVzb2x2ZWQoKSB7XG4gICAgICAgIHJldHVybiBQcm9taXNlLmFsbChbXG4gICAgICAgICAgICB0aGlzLm9uQmVmb3JlTmF2aWdhdGVFdmVudE5hdmlnYXRpb24sXG4gICAgICAgICAgICB0aGlzLm9uQ29tbWl0dGVkRXZlbnROYXZpZ2F0aW9uLFxuICAgICAgICBdKTtcbiAgICB9XG4gICAgLyoqXG4gICAgICogRWl0aGVyIHJldHVybnMgb3IgdGltZXMgb3V0IGFuZCByZXR1cm5zIHVuZGVmaW5lZCBvclxuICAgICAqIHJldHVybnMgdGhlIHJlc3VsdHMgZnJvbSByZXNvbHZlZCgpIGFib3ZlXG4gICAgICpcbiAgICAgKiBAcGFyYW0gbXNcbiAgICAgKi9cbiAgICBhc3luYyByZXNvbHZlZFdpdGhpblRpbWVvdXQobXMpIHtcbiAgICAgICAgY29uc3QgcmVzb2x2ZWQgPSBhd2FpdCBQcm9taXNlLnJhY2UoW1xuICAgICAgICAgICAgdGhpcy5yZXNvbHZlZCgpLFxuICAgICAgICAgICAgbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgbXMpKSxcbiAgICAgICAgXSk7XG4gICAgICAgIHJldHVybiByZXNvbHZlZDtcbiAgICB9XG59XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2ljR1Z1WkdsdVp5MXVZWFpwWjJGMGFXOXVMbXB6SWl3aWMyOTFjbU5sVW05dmRDSTZJaUlzSW5OdmRYSmpaWE1pT2xzaUxpNHZMaTR2TGk0dmMzSmpMMnhwWWk5d1pXNWthVzVuTFc1aGRtbG5ZWFJwYjI0dWRITWlYU3dpYm1GdFpYTWlPbHRkTENKdFlYQndhVzVuY3lJNklrRkJSVUU3TzBkQlJVYzdRVUZEU0N4TlFVRk5MRTlCUVU4c2FVSkJRV2xDTzBsQlExb3NLMEpCUVN0Q0xFTkJRWE5DTzBsQlEzSkVMREJDUVVFd1FpeERRVUZ6UWp0SlFVTjZSQ3h6UTBGQmMwTXNRMEZCWjBNN1NVRkRkRVVzYVVOQlFXbERMRU5CUVdkRE8wbEJRM2hGTzFGQlEwVXNTVUZCU1N4RFFVRkRMQ3RDUVVFclFpeEhRVUZITEVsQlFVa3NUMEZCVHl4RFFVRkRMRU5CUVVNc1QwRkJUeXhGUVVGRkxFVkJRVVU3V1VGRE4wUXNTVUZCU1N4RFFVRkRMSE5EUVVGelF5eEhRVUZITEU5QlFVOHNRMEZCUXp0UlFVTjRSQ3hEUVVGRExFTkJRVU1zUTBGQlF6dFJRVU5JTEVsQlFVa3NRMEZCUXl3d1FrRkJNRUlzUjBGQlJ5eEpRVUZKTEU5QlFVOHNRMEZCUXl4RFFVRkRMRTlCUVU4c1JVRkJSU3hGUVVGRk8xbEJRM2hFTEVsQlFVa3NRMEZCUXl4cFEwRkJhVU1zUjBGQlJ5eFBRVUZQTEVOQlFVTTdVVUZEYmtRc1EwRkJReXhEUVVGRExFTkJRVU03U1VGRFRDeERRVUZETzBsQlEwMHNVVUZCVVR0UlFVTmlMRTlCUVU4c1QwRkJUeXhEUVVGRExFZEJRVWNzUTBGQlF6dFpRVU5xUWl4SlFVRkpMRU5CUVVNc0swSkJRU3RDTzFsQlEzQkRMRWxCUVVrc1EwRkJReXd3UWtGQk1FSTdVMEZEYUVNc1EwRkJReXhEUVVGRE8wbEJRMHdzUTBGQlF6dEpRVVZFT3pzN096dFBRVXRITzBsQlEwa3NTMEZCU3l4RFFVRkRMSEZDUVVGeFFpeERRVUZETEVWQlFVVTdVVUZEYmtNc1RVRkJUU3hSUVVGUkxFZEJRVWNzVFVGQlRTeFBRVUZQTEVOQlFVTXNTVUZCU1N4RFFVRkRPMWxCUTJ4RExFbEJRVWtzUTBGQlF5eFJRVUZSTEVWQlFVVTdXVUZEWml4SlFVRkpMRTlCUVU4c1EwRkJReXhEUVVGRExFOUJRVThzUlVGQlJTeEZRVUZGTEVOQlFVTXNWVUZCVlN4RFFVRkRMRTlCUVU4c1JVRkJSU3hGUVVGRkxFTkJRVU1zUTBGQlF6dFRRVU5zUkN4RFFVRkRMRU5CUVVNN1VVRkRTQ3hQUVVGUExGRkJRVkVzUTBGQlF6dEpRVU5zUWl4RFFVRkRPME5CUTBZaWZRPT0iLCIvKipcbiAqIFRpZXMgdG9nZXRoZXIgdGhlIHR3byBzZXBhcmF0ZSBldmVudHMgdGhhdCB0b2dldGhlciBob2xkcyBpbmZvcm1hdGlvbiBhYm91dCBib3RoIHJlcXVlc3QgaGVhZGVycyBhbmQgYm9keVxuICovXG5leHBvcnQgY2xhc3MgUGVuZGluZ1JlcXVlc3Qge1xuICAgIG9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscztcbiAgICBvbkJlZm9yZVNlbmRIZWFkZXJzRXZlbnREZXRhaWxzO1xuICAgIHJlc29sdmVPbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHM7XG4gICAgcmVzb2x2ZU9uQmVmb3JlU2VuZEhlYWRlcnNFdmVudERldGFpbHM7XG4gICAgY29uc3RydWN0b3IoKSB7XG4gICAgICAgIHRoaXMub25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzID0gbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHtcbiAgICAgICAgICAgIHRoaXMucmVzb2x2ZU9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscyA9IHJlc29sdmU7XG4gICAgICAgIH0pO1xuICAgICAgICB0aGlzLm9uQmVmb3JlU2VuZEhlYWRlcnNFdmVudERldGFpbHMgPSBuZXcgUHJvbWlzZSgocmVzb2x2ZSkgPT4ge1xuICAgICAgICAgICAgdGhpcy5yZXNvbHZlT25CZWZvcmVTZW5kSGVhZGVyc0V2ZW50RGV0YWlscyA9IHJlc29sdmU7XG4gICAgICAgIH0pO1xuICAgIH1cbiAgICByZXNvbHZlZCgpIHtcbiAgICAgICAgcmV0dXJuIFByb21pc2UuYWxsKFtcbiAgICAgICAgICAgIHRoaXMub25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzLFxuICAgICAgICAgICAgdGhpcy5vbkJlZm9yZVNlbmRIZWFkZXJzRXZlbnREZXRhaWxzLFxuICAgICAgICBdKTtcbiAgICB9XG4gICAgLyoqXG4gICAgICogRWl0aGVyIHJldHVybnMgb3IgdGltZXMgb3V0IGFuZCByZXR1cm5zIHVuZGVmaW5lZCBvclxuICAgICAqIHJldHVybnMgdGhlIHJlc3VsdHMgZnJvbSByZXNvbHZlZCgpIGFib3ZlXG4gICAgICpcbiAgICAgKiBAcGFyYW0gbXNcbiAgICAgKi9cbiAgICBhc3luYyByZXNvbHZlZFdpdGhpblRpbWVvdXQobXMpIHtcbiAgICAgICAgY29uc3QgcmVzb2x2ZWQgPSBhd2FpdCBQcm9taXNlLnJhY2UoW1xuICAgICAgICAgICAgdGhpcy5yZXNvbHZlZCgpLFxuICAgICAgICAgICAgbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgbXMpKSxcbiAgICAgICAgXSk7XG4gICAgICAgIHJldHVybiByZXNvbHZlZDtcbiAgICB9XG59XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2ljR1Z1WkdsdVp5MXlaWEYxWlhOMExtcHpJaXdpYzI5MWNtTmxVbTl2ZENJNklpSXNJbk52ZFhKalpYTWlPbHNpTGk0dkxpNHZMaTR2YzNKakwyeHBZaTl3Wlc1a2FXNW5MWEpsY1hWbGMzUXVkSE1pWFN3aWJtRnRaWE1pT2x0ZExDSnRZWEJ3YVc1bmN5STZJa0ZCUzBFN08wZEJSVWM3UVVGRFNDeE5RVUZOTEU5QlFVOHNZMEZCWXp0SlFVTlVMREpDUVVFeVFpeERRVUZwUkR0SlFVTTFSU3dyUWtGQkswSXNRMEZCY1VRN1NVRkROMFlzYTBOQlFXdERMRU5CUlM5Q08wbEJRMGdzYzBOQlFYTkRMRU5CUlc1RE8wbEJRMVk3VVVGRFJTeEpRVUZKTEVOQlFVTXNNa0pCUVRKQ0xFZEJRVWNzU1VGQlNTeFBRVUZQTEVOQlFVTXNRMEZCUXl4UFFVRlBMRVZCUVVVc1JVRkJSVHRaUVVONlJDeEpRVUZKTEVOQlFVTXNhME5CUVd0RExFZEJRVWNzVDBGQlR5eERRVUZETzFGQlEzQkVMRU5CUVVNc1EwRkJReXhEUVVGRE8xRkJRMGdzU1VGQlNTeERRVUZETEN0Q1FVRXJRaXhIUVVGSExFbEJRVWtzVDBGQlR5eERRVUZETEVOQlFVTXNUMEZCVHl4RlFVRkZMRVZCUVVVN1dVRkROMFFzU1VGQlNTeERRVUZETEhORFFVRnpReXhIUVVGSExFOUJRVThzUTBGQlF6dFJRVU40UkN4RFFVRkRMRU5CUVVNc1EwRkJRenRKUVVOTUxFTkJRVU03U1VGRFRTeFJRVUZSTzFGQlEySXNUMEZCVHl4UFFVRlBMRU5CUVVNc1IwRkJSeXhEUVVGRE8xbEJRMnBDTEVsQlFVa3NRMEZCUXl3eVFrRkJNa0k3V1VGRGFFTXNTVUZCU1N4RFFVRkRMQ3RDUVVFclFqdFRRVU55UXl4RFFVRkRMRU5CUVVNN1NVRkRUQ3hEUVVGRE8wbEJSVVE3T3pzN08wOUJTMGM3U1VGRFNTeExRVUZMTEVOQlFVTXNjVUpCUVhGQ0xFTkJRVU1zUlVGQlJUdFJRVU51UXl4TlFVRk5MRkZCUVZFc1IwRkJSeXhOUVVGTkxFOUJRVThzUTBGQlF5eEpRVUZKTEVOQlFVTTdXVUZEYkVNc1NVRkJTU3hEUVVGRExGRkJRVkVzUlVGQlJUdFpRVU5tTEVsQlFVa3NUMEZCVHl4RFFVRkRMRU5CUVVNc1QwRkJUeXhGUVVGRkxFVkJRVVVzUTBGQlF5eFZRVUZWTEVOQlFVTXNUMEZCVHl4RlFVRkZMRVZCUVVVc1EwRkJReXhEUVVGRE8xTkJRMnhFTEVOQlFVTXNRMEZCUXp0UlFVTklMRTlCUVU4c1VVRkJVU3hEUVVGRE8wbEJRMnhDTEVOQlFVTTdRMEZEUmlKOSIsImltcG9ydCB7IFJlc3BvbnNlQm9keUxpc3RlbmVyIH0gZnJvbSBcIi4vcmVzcG9uc2UtYm9keS1saXN0ZW5lclwiO1xuLyoqXG4gKiBUaWVzIHRvZ2V0aGVyIHRoZSB0d28gc2VwYXJhdGUgZXZlbnRzIHRoYXQgdG9nZXRoZXIgaG9sZHMgaW5mb3JtYXRpb24gYWJvdXQgYm90aCByZXNwb25zZSBoZWFkZXJzIGFuZCBib2R5XG4gKi9cbmV4cG9ydCBjbGFzcyBQZW5kaW5nUmVzcG9uc2Uge1xuICAgIG9uQmVmb3JlUmVxdWVzdEV2ZW50RGV0YWlscztcbiAgICBvbkNvbXBsZXRlZEV2ZW50RGV0YWlscztcbiAgICByZXNwb25zZUJvZHlMaXN0ZW5lcjtcbiAgICByZXNvbHZlT25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzO1xuICAgIHJlc29sdmVPbkNvbXBsZXRlZEV2ZW50RGV0YWlscztcbiAgICBjb25zdHJ1Y3RvcigpIHtcbiAgICAgICAgdGhpcy5vbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHMgPSBuZXcgUHJvbWlzZSgocmVzb2x2ZSkgPT4ge1xuICAgICAgICAgICAgdGhpcy5yZXNvbHZlT25CZWZvcmVSZXF1ZXN0RXZlbnREZXRhaWxzID0gcmVzb2x2ZTtcbiAgICAgICAgfSk7XG4gICAgICAgIHRoaXMub25Db21wbGV0ZWRFdmVudERldGFpbHMgPSBuZXcgUHJvbWlzZSgocmVzb2x2ZSkgPT4ge1xuICAgICAgICAgICAgdGhpcy5yZXNvbHZlT25Db21wbGV0ZWRFdmVudERldGFpbHMgPSByZXNvbHZlO1xuICAgICAgICB9KTtcbiAgICB9XG4gICAgYWRkUmVzcG9uc2VSZXNwb25zZUJvZHlMaXN0ZW5lcihkZXRhaWxzKSB7XG4gICAgICAgIHRoaXMucmVzcG9uc2VCb2R5TGlzdGVuZXIgPSBuZXcgUmVzcG9uc2VCb2R5TGlzdGVuZXIoZGV0YWlscyk7XG4gICAgfVxuICAgIHJlc29sdmVkKCkge1xuICAgICAgICByZXR1cm4gUHJvbWlzZS5hbGwoW1xuICAgICAgICAgICAgdGhpcy5vbkJlZm9yZVJlcXVlc3RFdmVudERldGFpbHMsXG4gICAgICAgICAgICB0aGlzLm9uQ29tcGxldGVkRXZlbnREZXRhaWxzLFxuICAgICAgICBdKTtcbiAgICB9XG4gICAgLyoqXG4gICAgICogRWl0aGVyIHJldHVybnMgb3IgdGltZXMgb3V0IGFuZCByZXR1cm5zIHVuZGVmaW5lZCBvclxuICAgICAqIHJldHVybnMgdGhlIHJlc3VsdHMgZnJvbSByZXNvbHZlZCgpIGFib3ZlXG4gICAgICpcbiAgICAgKiBAcGFyYW0gbXNcbiAgICAgKi9cbiAgICBhc3luYyByZXNvbHZlZFdpdGhpblRpbWVvdXQobXMpIHtcbiAgICAgICAgY29uc3QgcmVzb2x2ZWQgPSBhd2FpdCBQcm9taXNlLnJhY2UoW1xuICAgICAgICAgICAgdGhpcy5yZXNvbHZlZCgpLFxuICAgICAgICAgICAgbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgbXMpKSxcbiAgICAgICAgXSk7XG4gICAgICAgIHJldHVybiByZXNvbHZlZDtcbiAgICB9XG59XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2ljR1Z1WkdsdVp5MXlaWE53YjI1elpTNXFjeUlzSW5OdmRYSmpaVkp2YjNRaU9pSWlMQ0p6YjNWeVkyVnpJanBiSWk0dUx5NHVMeTR1TDNOeVl5OXNhV0l2Y0dWdVpHbHVaeTF5WlhOd2IyNXpaUzUwY3lKZExDSnVZVzFsY3lJNlcxMHNJbTFoY0hCcGJtZHpJam9pUVVGSlFTeFBRVUZQTEVWQlFVVXNiMEpCUVc5Q0xFVkJRVVVzVFVGQlRTd3dRa0ZCTUVJc1EwRkJRenRCUVVWb1JUczdSMEZGUnp0QlFVTklMRTFCUVUwc1QwRkJUeXhsUVVGbE8wbEJRMVlzTWtKQlFUSkNMRU5CUVdsRU8wbEJRelZGTEhWQ1FVRjFRaXhEUVVFMlF6dEpRVU0zUlN4dlFrRkJiMElzUTBGQmRVSTdTVUZETTBNc2EwTkJRV3RETEVOQlJTOUNPMGxCUTBnc09FSkJRVGhDTEVOQlJUTkNPMGxCUTFZN1VVRkRSU3hKUVVGSkxFTkJRVU1zTWtKQlFUSkNMRWRCUVVjc1NVRkJTU3hQUVVGUExFTkJRVU1zUTBGQlF5eFBRVUZQTEVWQlFVVXNSVUZCUlR0WlFVTjZSQ3hKUVVGSkxFTkJRVU1zYTBOQlFXdERMRWRCUVVjc1QwRkJUeXhEUVVGRE8xRkJRM0JFTEVOQlFVTXNRMEZCUXl4RFFVRkRPMUZCUTBnc1NVRkJTU3hEUVVGRExIVkNRVUYxUWl4SFFVRkhMRWxCUVVrc1QwRkJUeXhEUVVGRExFTkJRVU1zVDBGQlR5eEZRVUZGTEVWQlFVVTdXVUZEY2tRc1NVRkJTU3hEUVVGRExEaENRVUU0UWl4SFFVRkhMRTlCUVU4c1EwRkJRenRSUVVOb1JDeERRVUZETEVOQlFVTXNRMEZCUXp0SlFVTk1MRU5CUVVNN1NVRkRUU3dyUWtGQkswSXNRMEZEY0VNc1QwRkJPRU03VVVGRk9VTXNTVUZCU1N4RFFVRkRMRzlDUVVGdlFpeEhRVUZITEVsQlFVa3NiMEpCUVc5Q0xFTkJRVU1zVDBGQlR5eERRVUZETEVOQlFVTTdTVUZEYUVVc1EwRkJRenRKUVVOTkxGRkJRVkU3VVVGRFlpeFBRVUZQTEU5QlFVOHNRMEZCUXl4SFFVRkhMRU5CUVVNN1dVRkRha0lzU1VGQlNTeERRVUZETERKQ1FVRXlRanRaUVVOb1F5eEpRVUZKTEVOQlFVTXNkVUpCUVhWQ08xTkJRemRDTEVOQlFVTXNRMEZCUXp0SlFVTk1MRU5CUVVNN1NVRkZSRHM3T3pzN1QwRkxSenRKUVVOSkxFdEJRVXNzUTBGQlF5eHhRa0ZCY1VJc1EwRkJReXhGUVVGRk8xRkJRMjVETEUxQlFVMHNVVUZCVVN4SFFVRkhMRTFCUVUwc1QwRkJUeXhEUVVGRExFbEJRVWtzUTBGQlF6dFpRVU5zUXl4SlFVRkpMRU5CUVVNc1VVRkJVU3hGUVVGRk8xbEJRMllzU1VGQlNTeFBRVUZQTEVOQlFVTXNRMEZCUXl4UFFVRlBMRVZCUVVVc1JVRkJSU3hEUVVGRExGVkJRVlVzUTBGQlF5eFBRVUZQTEVWQlFVVXNSVUZCUlN4RFFVRkRMRU5CUVVNN1UwRkRiRVFzUTBGQlF5eERRVUZETzFGQlEwZ3NUMEZCVHl4UlFVRlJMRU5CUVVNN1NVRkRiRUlzUTBGQlF6dERRVU5HSW4wPSIsImltcG9ydCB7IGRpZ2VzdE1lc3NhZ2UgfSBmcm9tIFwiLi9zaGEyNTZcIjtcbmV4cG9ydCBjbGFzcyBSZXNwb25zZUJvZHlMaXN0ZW5lciB7XG4gICAgcmVzcG9uc2VCb2R5O1xuICAgIGNvbnRlbnRIYXNoO1xuICAgIHJlc29sdmVSZXNwb25zZUJvZHk7XG4gICAgcmVzb2x2ZUNvbnRlbnRIYXNoO1xuICAgIGNvbnN0cnVjdG9yKGRldGFpbHMpIHtcbiAgICAgICAgdGhpcy5yZXNwb25zZUJvZHkgPSBuZXcgUHJvbWlzZSgocmVzb2x2ZSkgPT4ge1xuICAgICAgICAgICAgdGhpcy5yZXNvbHZlUmVzcG9uc2VCb2R5ID0gcmVzb2x2ZTtcbiAgICAgICAgfSk7XG4gICAgICAgIHRoaXMuY29udGVudEhhc2ggPSBuZXcgUHJvbWlzZSgocmVzb2x2ZSkgPT4ge1xuICAgICAgICAgICAgdGhpcy5yZXNvbHZlQ29udGVudEhhc2ggPSByZXNvbHZlO1xuICAgICAgICB9KTtcbiAgICAgICAgLy8gVXNlZCB0byBwYXJzZSBSZXNwb25zZSBzdHJlYW1cbiAgICAgICAgY29uc3QgZmlsdGVyID0gYnJvd3Nlci53ZWJSZXF1ZXN0LmZpbHRlclJlc3BvbnNlRGF0YShkZXRhaWxzLnJlcXVlc3RJZC50b1N0cmluZygpKTtcbiAgICAgICAgbGV0IHJlc3BvbnNlQm9keSA9IG5ldyBVaW50OEFycmF5KCk7XG4gICAgICAgIGZpbHRlci5vbmRhdGEgPSAoZXZlbnQpID0+IHtcbiAgICAgICAgICAgIGRpZ2VzdE1lc3NhZ2UoZXZlbnQuZGF0YSkudGhlbigoZGlnZXN0KSA9PiB7XG4gICAgICAgICAgICAgICAgdGhpcy5yZXNvbHZlQ29udGVudEhhc2goZGlnZXN0KTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgY29uc3QgaW5jb21pbmcgPSBuZXcgVWludDhBcnJheShldmVudC5kYXRhKTtcbiAgICAgICAgICAgIGNvbnN0IHRtcCA9IG5ldyBVaW50OEFycmF5KHJlc3BvbnNlQm9keS5sZW5ndGggKyBpbmNvbWluZy5sZW5ndGgpO1xuICAgICAgICAgICAgdG1wLnNldChyZXNwb25zZUJvZHkpO1xuICAgICAgICAgICAgdG1wLnNldChpbmNvbWluZywgcmVzcG9uc2VCb2R5Lmxlbmd0aCk7XG4gICAgICAgICAgICByZXNwb25zZUJvZHkgPSB0bXA7XG4gICAgICAgICAgICBmaWx0ZXIud3JpdGUoZXZlbnQuZGF0YSk7XG4gICAgICAgIH07XG4gICAgICAgIGZpbHRlci5vbnN0b3AgPSAoX2V2ZW50KSA9PiB7XG4gICAgICAgICAgICB0aGlzLnJlc29sdmVSZXNwb25zZUJvZHkocmVzcG9uc2VCb2R5KTtcbiAgICAgICAgICAgIGZpbHRlci5kaXNjb25uZWN0KCk7XG4gICAgICAgIH07XG4gICAgfVxuICAgIGFzeW5jIGdldFJlc3BvbnNlQm9keSgpIHtcbiAgICAgICAgcmV0dXJuIHRoaXMucmVzcG9uc2VCb2R5O1xuICAgIH1cbiAgICBhc3luYyBnZXRDb250ZW50SGFzaCgpIHtcbiAgICAgICAgcmV0dXJuIHRoaXMuY29udGVudEhhc2g7XG4gICAgfVxufVxuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pY21WemNHOXVjMlV0WW05a2VTMXNhWE4wWlc1bGNpNXFjeUlzSW5OdmRYSmpaVkp2YjNRaU9pSWlMQ0p6YjNWeVkyVnpJanBiSWk0dUx5NHVMeTR1TDNOeVl5OXNhV0l2Y21WemNHOXVjMlV0WW05a2VTMXNhWE4wWlc1bGNpNTBjeUpkTENKdVlXMWxjeUk2VzEwc0ltMWhjSEJwYm1keklqb2lRVUZEUVN4UFFVRlBMRVZCUVVVc1lVRkJZU3hGUVVGRkxFMUJRVTBzVlVGQlZTeERRVUZETzBGQlJYcERMRTFCUVUwc1QwRkJUeXh2UWtGQmIwSTdTVUZEWkN4WlFVRlpMRU5CUVhOQ08wbEJRMnhETEZkQlFWY3NRMEZCYTBJN1NVRkRkRU1zYlVKQlFXMUNMRU5CUVhGRE8wbEJRM2hFTEd0Q1FVRnJRaXhEUVVGblF6dEpRVVV4UkN4WlFVRlpMRTlCUVRoRE8xRkJRM2hFTEVsQlFVa3NRMEZCUXl4WlFVRlpMRWRCUVVjc1NVRkJTU3hQUVVGUExFTkJRVU1zUTBGQlF5eFBRVUZQTEVWQlFVVXNSVUZCUlR0WlFVTXhReXhKUVVGSkxFTkJRVU1zYlVKQlFXMUNMRWRCUVVjc1QwRkJUeXhEUVVGRE8xRkJRM0pETEVOQlFVTXNRMEZCUXl4RFFVRkRPMUZCUTBnc1NVRkJTU3hEUVVGRExGZEJRVmNzUjBGQlJ5eEpRVUZKTEU5QlFVOHNRMEZCUXl4RFFVRkRMRTlCUVU4c1JVRkJSU3hGUVVGRk8xbEJRM3BETEVsQlFVa3NRMEZCUXl4clFrRkJhMElzUjBGQlJ5eFBRVUZQTEVOQlFVTTdVVUZEY0VNc1EwRkJReXhEUVVGRExFTkJRVU03VVVGRlNDeG5RMEZCWjBNN1VVRkRhRU1zVFVGQlRTeE5RVUZOTEVkQlFWRXNUMEZCVHl4RFFVRkRMRlZCUVZVc1EwRkJReXhyUWtGQmEwSXNRMEZEZGtRc1QwRkJUeXhEUVVGRExGTkJRVk1zUTBGQlF5eFJRVUZSTEVWQlFVVXNRMEZEZEVJc1EwRkJRenRSUVVWVUxFbEJRVWtzV1VGQldTeEhRVUZITEVsQlFVa3NWVUZCVlN4RlFVRkZMRU5CUVVNN1VVRkRjRU1zVFVGQlRTeERRVUZETEUxQlFVMHNSMEZCUnl4RFFVRkRMRXRCUVVzc1JVRkJSU3hGUVVGRk8xbEJRM2hDTEdGQlFXRXNRMEZCUXl4TFFVRkxMRU5CUVVNc1NVRkJTU3hEUVVGRExFTkJRVU1zU1VGQlNTeERRVUZETEVOQlFVTXNUVUZCVFN4RlFVRkZMRVZCUVVVN1owSkJRM2hETEVsQlFVa3NRMEZCUXl4clFrRkJhMElzUTBGQlF5eE5RVUZOTEVOQlFVTXNRMEZCUXp0WlFVTnNReXhEUVVGRExFTkJRVU1zUTBGQlF6dFpRVU5JTEUxQlFVMHNVVUZCVVN4SFFVRkhMRWxCUVVrc1ZVRkJWU3hEUVVGRExFdEJRVXNzUTBGQlF5eEpRVUZKTEVOQlFVTXNRMEZCUXp0WlFVTTFReXhOUVVGTkxFZEJRVWNzUjBGQlJ5eEpRVUZKTEZWQlFWVXNRMEZCUXl4WlFVRlpMRU5CUVVNc1RVRkJUU3hIUVVGSExGRkJRVkVzUTBGQlF5eE5RVUZOTEVOQlFVTXNRMEZCUXp0WlFVTnNSU3hIUVVGSExFTkJRVU1zUjBGQlJ5eERRVUZETEZsQlFWa3NRMEZCUXl4RFFVRkRPMWxCUTNSQ0xFZEJRVWNzUTBGQlF5eEhRVUZITEVOQlFVTXNVVUZCVVN4RlFVRkZMRmxCUVZrc1EwRkJReXhOUVVGTkxFTkJRVU1zUTBGQlF6dFpRVU4yUXl4WlFVRlpMRWRCUVVjc1IwRkJSeXhEUVVGRE8xbEJRMjVDTEUxQlFVMHNRMEZCUXl4TFFVRkxMRU5CUVVNc1MwRkJTeXhEUVVGRExFbEJRVWtzUTBGQlF5eERRVUZETzFGQlF6TkNMRU5CUVVNc1EwRkJRenRSUVVWR0xFMUJRVTBzUTBGQlF5eE5RVUZOTEVkQlFVY3NRMEZCUXl4TlFVRk5MRVZCUVVVc1JVRkJSVHRaUVVONlFpeEpRVUZKTEVOQlFVTXNiVUpCUVcxQ0xFTkJRVU1zV1VGQldTeERRVUZETEVOQlFVTTdXVUZEZGtNc1RVRkJUU3hEUVVGRExGVkJRVlVzUlVGQlJTeERRVUZETzFGQlEzUkNMRU5CUVVNc1EwRkJRenRKUVVOS0xFTkJRVU03U1VGRlRTeExRVUZMTEVOQlFVTXNaVUZCWlR0UlFVTXhRaXhQUVVGUExFbEJRVWtzUTBGQlF5eFpRVUZaTEVOQlFVTTdTVUZETTBJc1EwRkJRenRKUVVWTkxFdEJRVXNzUTBGQlF5eGpRVUZqTzFGQlEzcENMRTlCUVU4c1NVRkJTU3hEUVVGRExGZEJRVmNzUTBGQlF6dEpRVU14UWl4RFFVRkRPME5CUTBZaWZRPT0iLCIvKipcbiAqIENvZGUgZnJvbSB0aGUgZXhhbXBsZSBhdFxuICogaHR0cHM6Ly9kZXZlbG9wZXIubW96aWxsYS5vcmcvZW4tVVMvZG9jcy9XZWIvQVBJL1N1YnRsZUNyeXB0by9kaWdlc3RcbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRpZ2VzdE1lc3NhZ2UobXNnVWludDgpIHtcbiAgICBjb25zdCBoYXNoQnVmZmVyID0gYXdhaXQgY3J5cHRvLnN1YnRsZS5kaWdlc3QoXCJTSEEtMjU2XCIsIG1zZ1VpbnQ4KTsgLy8gaGFzaCB0aGUgbWVzc2FnZVxuICAgIGNvbnN0IGhhc2hBcnJheSA9IEFycmF5LmZyb20obmV3IFVpbnQ4QXJyYXkoaGFzaEJ1ZmZlcikpOyAvLyBjb252ZXJ0IGJ1ZmZlciB0byBieXRlIGFycmF5XG4gICAgY29uc3QgaGFzaEhleCA9IGhhc2hBcnJheVxuICAgICAgICAubWFwKChiKSA9PiBiLnRvU3RyaW5nKDE2KS5wYWRTdGFydCgyLCBcIjBcIikpXG4gICAgICAgIC5qb2luKFwiXCIpOyAvLyBjb252ZXJ0IGJ5dGVzIHRvIGhleCBzdHJpbmdcbiAgICByZXR1cm4gaGFzaEhleDtcbn1cbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWMyaGhNalUyTG1weklpd2ljMjkxY21ObFVtOXZkQ0k2SWlJc0luTnZkWEpqWlhNaU9sc2lMaTR2TGk0dkxpNHZjM0pqTDJ4cFlpOXphR0V5TlRZdWRITWlYU3dpYm1GdFpYTWlPbHRkTENKdFlYQndhVzVuY3lJNklrRkJRVUU3T3p0SFFVZEhPMEZCUlVnc1RVRkJUU3hEUVVGRExFdEJRVXNzVlVGQlZTeGhRVUZoTEVOQlFVTXNVVUZCYjBJN1NVRkRkRVFzVFVGQlRTeFZRVUZWTEVkQlFVY3NUVUZCVFN4TlFVRk5MRU5CUVVNc1RVRkJUU3hEUVVGRExFMUJRVTBzUTBGQlF5eFRRVUZUTEVWQlFVVXNVVUZCVVN4RFFVRkRMRU5CUVVNc1EwRkJReXh0UWtGQmJVSTdTVUZEZGtZc1RVRkJUU3hUUVVGVExFZEJRVWNzUzBGQlN5eERRVUZETEVsQlFVa3NRMEZCUXl4SlFVRkpMRlZCUVZVc1EwRkJReXhWUVVGVkxFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTXNLMEpCUVN0Q08wbEJRM3BHTEUxQlFVMHNUMEZCVHl4SFFVRkhMRk5CUVZNN1UwRkRkRUlzUjBGQlJ5eERRVUZETEVOQlFVTXNRMEZCUXl4RlFVRkZMRVZCUVVVc1EwRkJReXhEUVVGRExFTkJRVU1zVVVGQlVTeERRVUZETEVWQlFVVXNRMEZCUXl4RFFVRkRMRkZCUVZFc1EwRkJReXhEUVVGRExFVkJRVVVzUjBGQlJ5eERRVUZETEVOQlFVTTdVMEZETTBNc1NVRkJTU3hEUVVGRExFVkJRVVVzUTBGQlF5eERRVUZETEVOQlFVTXNPRUpCUVRoQ08wbEJRek5ETEU5QlFVOHNUMEZCVHl4RFFVRkRPMEZCUTJwQ0xFTkJRVU1pZlE9PSIsImV4cG9ydCBmdW5jdGlvbiBlbmNvZGVfdXRmOChzKSB7XG4gICAgcmV0dXJuIHVuZXNjYXBlKGVuY29kZVVSSUNvbXBvbmVudChzKSk7XG59XG5leHBvcnQgY29uc3QgZXNjYXBlU3RyaW5nID0gZnVuY3Rpb24gKHN0cikge1xuICAgIC8vIENvbnZlcnQgdG8gc3RyaW5nIGlmIG5lY2Vzc2FyeVxuICAgIGlmICh0eXBlb2Ygc3RyICE9PSBcInN0cmluZ1wiKSB7XG4gICAgICAgIHN0ciA9IFN0cmluZyhzdHIpO1xuICAgIH1cbiAgICByZXR1cm4gZW5jb2RlX3V0Zjgoc3RyKTtcbn07XG5leHBvcnQgY29uc3QgZXNjYXBlVXJsID0gZnVuY3Rpb24gKHVybCwgc3RyaXBEYXRhVXJsRGF0YSA9IHRydWUpIHtcbiAgICB1cmwgPSBlc2NhcGVTdHJpbmcodXJsKTtcbiAgICAvLyBkYXRhOls8bWVkaWF0eXBlPl1bO2Jhc2U2NF0sPGRhdGE+XG4gICAgaWYgKHVybC5zdWJzdHIoMCwgNSkgPT09IFwiZGF0YTpcIiAmJlxuICAgICAgICBzdHJpcERhdGFVcmxEYXRhICYmXG4gICAgICAgIHVybC5pbmRleE9mKFwiLFwiKSA+IC0xKSB7XG4gICAgICAgIHVybCA9IHVybC5zdWJzdHIoMCwgdXJsLmluZGV4T2YoXCIsXCIpICsgMSkgKyBcIjxkYXRhLXN0cmlwcGVkPlwiO1xuICAgIH1cbiAgICByZXR1cm4gdXJsO1xufTtcbi8vIEJhc2U2NCBlbmNvZGluZywgZm91bmQgb246XG4vLyBodHRwczovL3N0YWNrb3ZlcmZsb3cuY29tL3F1ZXN0aW9ucy8xMjcxMDAwMS9ob3ctdG8tY29udmVydC11aW50OC1hcnJheS10by1iYXNlNjQtZW5jb2RlZC1zdHJpbmcvMjU2NDQ0MDkjMjU2NDQ0MDlcbmV4cG9ydCBjb25zdCBVaW50OFRvQmFzZTY0ID0gZnVuY3Rpb24gKHU4QXJyKSB7XG4gICAgY29uc3QgQ0hVTktfU0laRSA9IDB4ODAwMDsgLy8gYXJiaXRyYXJ5IG51bWJlclxuICAgIGxldCBpbmRleCA9IDA7XG4gICAgY29uc3QgbGVuZ3RoID0gdThBcnIubGVuZ3RoO1xuICAgIGxldCByZXN1bHQgPSBcIlwiO1xuICAgIGxldCBzbGljZTtcbiAgICB3aGlsZSAoaW5kZXggPCBsZW5ndGgpIHtcbiAgICAgICAgc2xpY2UgPSB1OEFyci5zdWJhcnJheShpbmRleCwgTWF0aC5taW4oaW5kZXggKyBDSFVOS19TSVpFLCBsZW5ndGgpKTtcbiAgICAgICAgcmVzdWx0ICs9IFN0cmluZy5mcm9tQ2hhckNvZGUuYXBwbHkobnVsbCwgc2xpY2UpO1xuICAgICAgICBpbmRleCArPSBDSFVOS19TSVpFO1xuICAgIH1cbiAgICByZXR1cm4gYnRvYShyZXN1bHQpO1xufTtcbmV4cG9ydCBjb25zdCBib29sVG9JbnQgPSBmdW5jdGlvbiAoYm9vbCkge1xuICAgIHJldHVybiBib29sID8gMSA6IDA7XG59O1xuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pYzNSeWFXNW5MWFYwYVd4ekxtcHpJaXdpYzI5MWNtTmxVbTl2ZENJNklpSXNJbk52ZFhKalpYTWlPbHNpTGk0dkxpNHZMaTR2YzNKakwyeHBZaTl6ZEhKcGJtY3RkWFJwYkhNdWRITWlYU3dpYm1GdFpYTWlPbHRkTENKdFlYQndhVzVuY3lJNklrRkJRVUVzVFVGQlRTeFZRVUZWTEZkQlFWY3NRMEZCUXl4RFFVRkRPMGxCUXpOQ0xFOUJRVThzVVVGQlVTeERRVUZETEd0Q1FVRnJRaXhEUVVGRExFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTTdRVUZEZWtNc1EwRkJRenRCUVVWRUxFMUJRVTBzUTBGQlF5eE5RVUZOTEZsQlFWa3NSMEZCUnl4VlFVRlZMRWRCUVZFN1NVRkROVU1zYVVOQlFXbERPMGxCUTJwRExFbEJRVWtzVDBGQlR5eEhRVUZITEV0QlFVc3NVVUZCVVN4RlFVRkZPMUZCUXpOQ0xFZEJRVWNzUjBGQlJ5eE5RVUZOTEVOQlFVTXNSMEZCUnl4RFFVRkRMRU5CUVVNN1MwRkRia0k3U1VGRlJDeFBRVUZQTEZkQlFWY3NRMEZCUXl4SFFVRkhMRU5CUVVNc1EwRkJRenRCUVVNeFFpeERRVUZETEVOQlFVTTdRVUZGUml4TlFVRk5MRU5CUVVNc1RVRkJUU3hUUVVGVExFZEJRVWNzVlVGRGRrSXNSMEZCVnl4RlFVTllMRzFDUVVFMFFpeEpRVUZKTzBsQlJXaERMRWRCUVVjc1IwRkJSeXhaUVVGWkxFTkJRVU1zUjBGQlJ5eERRVUZETEVOQlFVTTdTVUZEZUVJc2NVTkJRWEZETzBsQlEzSkRMRWxCUTBVc1IwRkJSeXhEUVVGRExFMUJRVTBzUTBGQlF5eERRVUZETEVWQlFVVXNRMEZCUXl4RFFVRkRMRXRCUVVzc1QwRkJUenRSUVVNMVFpeG5Ra0ZCWjBJN1VVRkRhRUlzUjBGQlJ5eERRVUZETEU5QlFVOHNRMEZCUXl4SFFVRkhMRU5CUVVNc1IwRkJSeXhEUVVGRExFTkJRVU1zUlVGRGNrSTdVVUZEUVN4SFFVRkhMRWRCUVVjc1IwRkJSeXhEUVVGRExFMUJRVTBzUTBGQlF5eERRVUZETEVWQlFVVXNSMEZCUnl4RFFVRkRMRTlCUVU4c1EwRkJReXhIUVVGSExFTkJRVU1zUjBGQlJ5eERRVUZETEVOQlFVTXNSMEZCUnl4cFFrRkJhVUlzUTBGQlF6dExRVU12UkR0SlFVTkVMRTlCUVU4c1IwRkJSeXhEUVVGRE8wRkJRMklzUTBGQlF5eERRVUZETzBGQlJVWXNOa0pCUVRaQ08wRkJRemRDTEhGSVFVRnhTRHRCUVVOeVNDeE5RVUZOTEVOQlFVTXNUVUZCVFN4aFFVRmhMRWRCUVVjc1ZVRkJWU3hMUVVGcFFqdEpRVU4wUkN4TlFVRk5MRlZCUVZVc1IwRkJSeXhOUVVGTkxFTkJRVU1zUTBGQlF5eHRRa0ZCYlVJN1NVRkRPVU1zU1VGQlNTeExRVUZMTEVkQlFVY3NRMEZCUXl4RFFVRkRPMGxCUTJRc1RVRkJUU3hOUVVGTkxFZEJRVWNzUzBGQlN5eERRVUZETEUxQlFVMHNRMEZCUXp0SlFVTTFRaXhKUVVGSkxFMUJRVTBzUjBGQlJ5eEZRVUZGTEVOQlFVTTdTVUZEYUVJc1NVRkJTU3hMUVVGcFFpeERRVUZETzBsQlEzUkNMRTlCUVU4c1MwRkJTeXhIUVVGSExFMUJRVTBzUlVGQlJUdFJRVU55UWl4TFFVRkxMRWRCUVVjc1MwRkJTeXhEUVVGRExGRkJRVkVzUTBGQlF5eExRVUZMTEVWQlFVVXNTVUZCU1N4RFFVRkRMRWRCUVVjc1EwRkJReXhMUVVGTExFZEJRVWNzVlVGQlZTeEZRVUZGTEUxQlFVMHNRMEZCUXl4RFFVRkRMRU5CUVVNN1VVRkRjRVVzVFVGQlRTeEpRVUZKTEUxQlFVMHNRMEZCUXl4WlFVRlpMRU5CUVVNc1MwRkJTeXhEUVVGRExFbEJRVWtzUlVGQlJTeExRVUZMTEVOQlFVTXNRMEZCUXp0UlFVTnFSQ3hMUVVGTExFbEJRVWtzVlVGQlZTeERRVUZETzB0QlEzSkNPMGxCUTBRc1QwRkJUeXhKUVVGSkxFTkJRVU1zVFVGQlRTeERRVUZETEVOQlFVTTdRVUZEZEVJc1EwRkJReXhEUVVGRE8wRkJSVVlzVFVGQlRTeERRVUZETEUxQlFVMHNVMEZCVXl4SFFVRkhMRlZCUVZVc1NVRkJZVHRKUVVNNVF5eFBRVUZQTEVsQlFVa3NRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTTdRVUZEZEVJc1EwRkJReXhEUVVGREluMD0iLCIvKiBlc2xpbnQtZGlzYWJsZSBuby1iaXR3aXNlICovXG4vLyBmcm9tIGh0dHBzOi8vZ2lzdC5naXRodWIuY29tL2plZC85ODI4ODMjZ2lzdGNvbW1lbnQtMjQwMzM2OVxuY29uc3QgaGV4ID0gW107XG5mb3IgKGxldCBpID0gMDsgaSA8IDI1NjsgaSsrKSB7XG4gICAgaGV4W2ldID0gKGkgPCAxNiA/IFwiMFwiIDogXCJcIikgKyBpLnRvU3RyaW5nKDE2KTtcbn1cbmV4cG9ydCBjb25zdCBtYWtlVVVJRCA9ICgpID0+IHtcbiAgICBjb25zdCByID0gY3J5cHRvLmdldFJhbmRvbVZhbHVlcyhuZXcgVWludDhBcnJheSgxNikpO1xuICAgIHJbNl0gPSAocls2XSAmIDB4MGYpIHwgMHg0MDtcbiAgICByWzhdID0gKHJbOF0gJiAweDNmKSB8IDB4ODA7XG4gICAgcmV0dXJuIChoZXhbclswXV0gK1xuICAgICAgICBoZXhbclsxXV0gK1xuICAgICAgICBoZXhbclsyXV0gK1xuICAgICAgICBoZXhbclszXV0gK1xuICAgICAgICBcIi1cIiArXG4gICAgICAgIGhleFtyWzRdXSArXG4gICAgICAgIGhleFtyWzVdXSArXG4gICAgICAgIFwiLVwiICtcbiAgICAgICAgaGV4W3JbNl1dICtcbiAgICAgICAgaGV4W3JbN11dICtcbiAgICAgICAgXCItXCIgK1xuICAgICAgICBoZXhbcls4XV0gK1xuICAgICAgICBoZXhbcls5XV0gK1xuICAgICAgICBcIi1cIiArXG4gICAgICAgIGhleFtyWzEwXV0gK1xuICAgICAgICBoZXhbclsxMV1dICtcbiAgICAgICAgaGV4W3JbMTJdXSArXG4gICAgICAgIGhleFtyWzEzXV0gK1xuICAgICAgICBoZXhbclsxNF1dICtcbiAgICAgICAgaGV4W3JbMTVdXSk7XG59O1xuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pZFhWcFpDNXFjeUlzSW5OdmRYSmpaVkp2YjNRaU9pSWlMQ0p6YjNWeVkyVnpJanBiSWk0dUx5NHVMeTR1TDNOeVl5OXNhV0l2ZFhWcFpDNTBjeUpkTENKdVlXMWxjeUk2VzEwc0ltMWhjSEJwYm1keklqb2lRVUZCUVN3clFrRkJLMEk3UVVGRkwwSXNPRVJCUVRoRU8wRkJRemxFTEUxQlFVMHNSMEZCUnl4SFFVRkhMRVZCUVVVc1EwRkJRenRCUVVWbUxFdEJRVXNzU1VGQlNTeERRVUZETEVkQlFVY3NRMEZCUXl4RlFVRkZMRU5CUVVNc1IwRkJSeXhIUVVGSExFVkJRVVVzUTBGQlF5eEZRVUZGTEVWQlFVVTdTVUZETlVJc1IwRkJSeXhEUVVGRExFTkJRVU1zUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXl4SFFVRkhMRVZCUVVVc1EwRkJReXhEUVVGRExFTkJRVU1zUjBGQlJ5eERRVUZETEVOQlFVTXNRMEZCUXl4RlFVRkZMRU5CUVVNc1IwRkJSeXhEUVVGRExFTkJRVU1zVVVGQlVTeERRVUZETEVWQlFVVXNRMEZCUXl4RFFVRkRPME5CUXk5RE8wRkJSVVFzVFVGQlRTeERRVUZETEUxQlFVMHNVVUZCVVN4SFFVRkhMRWRCUVVjc1JVRkJSVHRKUVVNelFpeE5RVUZOTEVOQlFVTXNSMEZCUnl4TlFVRk5MRU5CUVVNc1pVRkJaU3hEUVVGRExFbEJRVWtzVlVGQlZTeERRVUZETEVWQlFVVXNRMEZCUXl4RFFVRkRMRU5CUVVNN1NVRkZja1FzUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4SFFVRkhMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU1zUTBGQlF5eEhRVUZITEVsQlFVa3NRMEZCUXl4SFFVRkhMRWxCUVVrc1EwRkJRenRKUVVNMVFpeERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRMRWRCUVVjc1EwRkJReXhEUVVGRExFTkJRVU1zUTBGQlF5eERRVUZETEVkQlFVY3NTVUZCU1N4RFFVRkRMRWRCUVVjc1NVRkJTU3hEUVVGRE8wbEJSVFZDTEU5QlFVOHNRMEZEVEN4SFFVRkhMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU1zUTBGQlF5eERRVUZETzFGQlExUXNSMEZCUnl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU1zUTBGQlF6dFJRVU5VTEVkQlFVY3NRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU03VVVGRFZDeEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRE8xRkJRMVFzUjBGQlJ6dFJRVU5JTEVkQlFVY3NRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU03VVVGRFZDeEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRE8xRkJRMVFzUjBGQlJ6dFJRVU5JTEVkQlFVY3NRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU03VVVGRFZDeEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRE8xRkJRMVFzUjBGQlJ6dFJRVU5JTEVkQlFVY3NRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU03VVVGRFZDeEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRE8xRkJRMVFzUjBGQlJ6dFJRVU5JTEVkQlFVY3NRMEZCUXl4RFFVRkRMRU5CUVVNc1JVRkJSU3hEUVVGRExFTkJRVU03VVVGRFZpeEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRMRVZCUVVVc1EwRkJReXhEUVVGRE8xRkJRMVlzUjBGQlJ5eERRVUZETEVOQlFVTXNRMEZCUXl4RlFVRkZMRU5CUVVNc1EwRkJRenRSUVVOV0xFZEJRVWNzUTBGQlF5eERRVUZETEVOQlFVTXNSVUZCUlN4RFFVRkRMRU5CUVVNN1VVRkRWaXhIUVVGSExFTkJRVU1zUTBGQlF5eERRVUZETEVWQlFVVXNRMEZCUXl4RFFVRkRPMUZCUTFZc1IwRkJSeXhEUVVGRExFTkJRVU1zUTBGQlF5eEZRVUZGTEVOQlFVTXNRMEZCUXl4RFFVTllMRU5CUVVNN1FVRkRTaXhEUVVGRExFTkJRVU1pZlE9PSIsIi8vIGh0dHBzOi8vd3d3LnVuaWNvZGUub3JnL3JlcG9ydHMvdHIzNS90cjM1LWRhdGVzLmh0bWwjRGF0ZV9GaWVsZF9TeW1ib2xfVGFibGVcbmV4cG9ydCBjb25zdCBkYXRlVGltZVVuaWNvZGVGb3JtYXRTdHJpbmcgPSBcInl5eXktTU0tZGQnVCdISDptbTpzcy5TU1NYWFwiO1xuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pYzJOb1pXMWhMbXB6SWl3aWMyOTFjbU5sVW05dmRDSTZJaUlzSW5OdmRYSmpaWE1pT2xzaUxpNHZMaTR2YzNKakwzTmphR1Z0WVM1MGN5SmRMQ0p1WVcxbGN5STZXMTBzSW0xaGNIQnBibWR6SWpvaVFVRkpRU3dyUlVGQkswVTdRVUZETDBVc1RVRkJUU3hEUVVGRExFMUJRVTBzTWtKQlFUSkNMRWRCUVVjc05rSkJRVFpDTEVOQlFVTWlmUT09IiwiLy8gVGhlIG1vZHVsZSBjYWNoZVxudmFyIF9fd2VicGFja19tb2R1bGVfY2FjaGVfXyA9IHt9O1xuXG4vLyBUaGUgcmVxdWlyZSBmdW5jdGlvblxuZnVuY3Rpb24gX193ZWJwYWNrX3JlcXVpcmVfXyhtb2R1bGVJZCkge1xuXHQvLyBDaGVjayBpZiBtb2R1bGUgaXMgaW4gY2FjaGVcblx0dmFyIGNhY2hlZE1vZHVsZSA9IF9fd2VicGFja19tb2R1bGVfY2FjaGVfX1ttb2R1bGVJZF07XG5cdGlmIChjYWNoZWRNb2R1bGUgIT09IHVuZGVmaW5lZCkge1xuXHRcdHJldHVybiBjYWNoZWRNb2R1bGUuZXhwb3J0cztcblx0fVxuXHQvLyBDcmVhdGUgYSBuZXcgbW9kdWxlIChhbmQgcHV0IGl0IGludG8gdGhlIGNhY2hlKVxuXHR2YXIgbW9kdWxlID0gX193ZWJwYWNrX21vZHVsZV9jYWNoZV9fW21vZHVsZUlkXSA9IHtcblx0XHQvLyBubyBtb2R1bGUuaWQgbmVlZGVkXG5cdFx0Ly8gbm8gbW9kdWxlLmxvYWRlZCBuZWVkZWRcblx0XHRleHBvcnRzOiB7fVxuXHR9O1xuXG5cdC8vIEV4ZWN1dGUgdGhlIG1vZHVsZSBmdW5jdGlvblxuXHRfX3dlYnBhY2tfbW9kdWxlc19fW21vZHVsZUlkXShtb2R1bGUsIG1vZHVsZS5leHBvcnRzLCBfX3dlYnBhY2tfcmVxdWlyZV9fKTtcblxuXHQvLyBSZXR1cm4gdGhlIGV4cG9ydHMgb2YgdGhlIG1vZHVsZVxuXHRyZXR1cm4gbW9kdWxlLmV4cG9ydHM7XG59XG5cbiIsIi8vIGRlZmluZSBnZXR0ZXIgZnVuY3Rpb25zIGZvciBoYXJtb255IGV4cG9ydHNcbl9fd2VicGFja19yZXF1aXJlX18uZCA9IChleHBvcnRzLCBkZWZpbml0aW9uKSA9PiB7XG5cdGZvcih2YXIga2V5IGluIGRlZmluaXRpb24pIHtcblx0XHRpZihfX3dlYnBhY2tfcmVxdWlyZV9fLm8oZGVmaW5pdGlvbiwga2V5KSAmJiAhX193ZWJwYWNrX3JlcXVpcmVfXy5vKGV4cG9ydHMsIGtleSkpIHtcblx0XHRcdE9iamVjdC5kZWZpbmVQcm9wZXJ0eShleHBvcnRzLCBrZXksIHsgZW51bWVyYWJsZTogdHJ1ZSwgZ2V0OiBkZWZpbml0aW9uW2tleV0gfSk7XG5cdFx0fVxuXHR9XG59OyIsIl9fd2VicGFja19yZXF1aXJlX18ubyA9IChvYmosIHByb3ApID0+IChPYmplY3QucHJvdG90eXBlLmhhc093blByb3BlcnR5LmNhbGwob2JqLCBwcm9wKSkiLCIvLyBkZWZpbmUgX19lc01vZHVsZSBvbiBleHBvcnRzXG5fX3dlYnBhY2tfcmVxdWlyZV9fLnIgPSAoZXhwb3J0cykgPT4ge1xuXHRpZih0eXBlb2YgU3ltYm9sICE9PSAndW5kZWZpbmVkJyAmJiBTeW1ib2wudG9TdHJpbmdUYWcpIHtcblx0XHRPYmplY3QuZGVmaW5lUHJvcGVydHkoZXhwb3J0cywgU3ltYm9sLnRvU3RyaW5nVGFnLCB7IHZhbHVlOiAnTW9kdWxlJyB9KTtcblx0fVxuXHRPYmplY3QuZGVmaW5lUHJvcGVydHkoZXhwb3J0cywgJ19fZXNNb2R1bGUnLCB7IHZhbHVlOiB0cnVlIH0pO1xufTsiLCJpbXBvcnQge2luamVjdEphdmFzY3JpcHRJbnN0cnVtZW50UGFnZVNjcmlwdH0gZnJvbSBcIm9wZW53cG0td2ViZXh0LWluc3RydW1lbnRhdGlvblwiO1xuXG5pbmplY3RKYXZhc2NyaXB0SW5zdHJ1bWVudFBhZ2VTY3JpcHQod2luZG93Lm9wZW5XcG1Db250ZW50U2NyaXB0Q29uZmlnIHx8IHt9KTtcbmRlbGV0ZSB3aW5kb3cub3BlbldwbUNvbnRlbnRTY3JpcHRDb25maWc7XG4iXSwic291cmNlUm9vdCI6IiJ9