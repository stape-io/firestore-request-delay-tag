const Firestore = require('Firestore');
const runContainer = require('runContainer');
const makeInteger = require('makeInteger');
const getTimestampMillis = require('getTimestampMillis');
const getAllEventData = require('getAllEventData');
const Promise = require('Promise');
const JSON = require('JSON');
const makeString = require('makeString');
const logToConsole = require('logToConsole');
const getRequestHeader = require('getRequestHeader');
const getContainerVersion = require('getContainerVersion');

const isLoggingEnabled = determinateIsLoggingEnabled();
const traceId = isLoggingEnabled ? getRequestHeader('trace-id') : undefined;

const limit = data.limit;
const custom_event_name = data.custom_event_name;
const input_event_data = getAllEventData();
const replay_key = data.replay_key;
const replay_value = makeString(data.replay_value);
const delay_minutes = makeInteger(data.delay);
const store_key = data.store_key;

const max_ts = getTimestampMillis() - (1000 * 60 * delay_minutes);
const queries = [['processed', '==', false]];

if (data.type === 'fetch') {
  fetchEvents();
} else if (data.type === 'store') {
  storeEvent();
} else if (data.type === 'replay') {
  replayEvent();
}

function fetchEvents() {
  let firebaseOptions = {limit: makeInteger(limit)};
  if (data.firebaseProjectId) firebaseOptions.projectId = data.firebaseProjectId;

  Firestore.query(data.firebasePath, queries, firebaseOptions).then(function (documents) {
    const promises = documents.map(function (document) {
      if (!document.data) {
        if (isLoggingEnabled) {
          logToConsole(
            JSON.stringify({
              Name: 'FirestoreRequestDelay',
              Type: 'Message',
              TraceId: traceId,
              EventName: 'Error',
              Message: 'Document data is null or undefined: ' + document.id,
            })
          );
        }

        return Promise.create(function (resolve) { resolve(); });
      }

      let event = document.data.event_data || {};
      event.event_name = custom_event_name || event.event_name;

      if (event.event_name === input_event_data.event_name) {
        if (isLoggingEnabled) {
          logToConsole(
            JSON.stringify({
              Name: 'FirestoreRequestDelay',
              Type: 'Message',
              TraceId: traceId,
              EventName: 'Error',
              Message: 'Output event_name is the same as input_event_data.event_name. Potential loop detected.',
            })
          );
        }

        return Promise.create(function (resolve, reject) { reject('Potential loop detected.'); });
      }

      if (document.data.timestamp < max_ts) {
        const documentPath = document.id;

        if (!documentPath) {
          if (isLoggingEnabled) {
            logToConsole(
              JSON.stringify({
                Name: 'FirestoreRequestDelay',
                Type: 'Message',
                TraceId: traceId,
                EventName: 'Error',
                Message: 'Document path is null or undefined: ' + documentPath,
              })
            );
          }

          return Promise.create(function (resolve, reject) { reject('Document path is null or undefined.'); });
        }

        let firebaseOptions = {merge: true};
        if (data.firebaseProjectId) firebaseOptions.projectId = data.firebaseProjectId;

        return Firestore.write(documentPath, { 'processed': true }, firebaseOptions).then(function () {
          return Promise.create(function (resolve) {
            runContainer(event);
            resolve();
          });
        }).catch(data.gtmOnFailure);
      } else {
        return Promise.create(function (resolve) { resolve(); });
      }
    });

    Promise.all(promises).then(function () {
      data.gtmOnSuccess();
    }).catch(function (error) {
      if (isLoggingEnabled) {
        logToConsole(
          JSON.stringify({
            Name: 'FirestoreRequestDelay',
            Type: 'Message',
            TraceId: traceId,
            EventName: 'Error',
            Message: 'Firebase: ' + error,
          })
        );
      }

      data.gtmOnFailure();
    });
  }, data.gtmOnFailure);
}

function storeEvent() {
  if (input_event_data.event_name === custom_event_name) {
    if (isLoggingEnabled) {
      logToConsole(
        JSON.stringify({
          Name: 'FirestoreRequestDelay',
          Type: 'Message',
          TraceId: traceId,
          EventName: 'Error',
          Message: 'Input event_name is the same as custom_event_name. Potential loop detected.',
        })
      );
    }

    data.gtmOnFailure();

    return;
  }

  let documentData = {
    event_data: input_event_data,
    processed: false,
    timestamp: getTimestampMillis()
  };

  if (store_key && input_event_data[store_key] !== undefined) {
    documentData[store_key] = JSON.stringify(input_event_data[store_key]);
  }

  let firebaseOptions = {};
  if (data.firebaseProjectId) firebaseOptions.projectId = data.firebaseProjectId;

  Firestore.write(data.firebasePath, documentData, firebaseOptions).then((id) => {
    if (isLoggingEnabled) {
      logToConsole(
        JSON.stringify({
          Name: 'FirestoreRequestDelay',
          Type: 'Response',
          TraceId: traceId,
          EventName: 'StapeStoreReStorePost',
          ResponseStatusCode: 200,
          ResponseBody: {id: id},
        })
      );
    }

    data.gtmOnSuccess();
  }, data.gtmOnFailure);
}

function replayEvent() {
  const replayQuery = [[replay_key, '==', replay_value]];

  if (isLoggingEnabled) {
    logToConsole(
      JSON.stringify({
        Name: 'FirestoreRequestDelay',
        Type: 'Request',
        TraceId: traceId,
        EventName: 'FirestoreRequestDelay',
        RequestMethod: 'GET',
        RequestUrl: data.firebasePath,
        RequestBody: replayQuery,
      })
    );
  }

  let firebaseOptions = { limit: 1 };
  if (data.firebaseProjectId) firebaseOptions.projectId = data.firebaseProjectId;

  Firestore.query(data.firebasePath, replayQuery, firebaseOptions).then(function (documents) {
    let document = documents[0];

    if (!document) {
      if (isLoggingEnabled) {
        logToConsole(
          JSON.stringify({
            Name: 'FirestoreRequestDelay',
            Type: 'Response',
            TraceId: traceId,
            EventName: 'StapeStoreReStorePost',
            ResponseStatusCode: 500,
            ResponseBody: 'No matching document found for replay.',
          })
        );
      }

      data.gtmOnFailure();
      return;
    }

    let event = document.data.event_data || {};
    event.event_name = custom_event_name || event.event_name;

    // Check for loop condition
    if (event.event_name === input_event_data.event_name) {
      if (isLoggingEnabled) {
        logToConsole(
          JSON.stringify({
            Name: 'FirestoreRequestDelay',
            Type: 'Response',
            TraceId: traceId,
            EventName: 'FirestoreRequestDelay',
            ResponseBody: 'Output event_name is the same as input_event_data.event_name. Potential loop detected.',
            ResponseStatusCode: 500,
          })
        );
      }

      data.gtmOnFailure();
      return;
    }

    runContainer(event);
    data.gtmOnSuccess();
  }).catch(function (error) {
    if (isLoggingEnabled) {
      logToConsole(
        JSON.stringify({
          Name: 'FirestoreRequestDelay',
          Type: 'Response',
          TraceId: traceId,
          EventName: 'FirestoreRequestDelay',
          ResponseBody: error,
          ResponseStatusCode: 500,
        })
      );
    }

    data.gtmOnFailure();
  });
}

function determinateIsLoggingEnabled() {
  const containerVersion = getContainerVersion();
  const isDebug = !!(containerVersion && (containerVersion.debugMode || containerVersion.previewMode));

  if (!data.logType) {
    return isDebug;
  }

  if (data.logType === 'no') {
    return false;
  }

  if (data.logType === 'debug') {
    return isDebug;
  }

  return data.logType === 'always';
}

