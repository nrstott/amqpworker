'use strict';

var _ = require('lodash');
var winston = require('winston');
var Promise = require('bluebird');

module.exports = Worker;

function Worker(amqplib, log) {
  log = log || winston;

  return Object.create({
    listen: function listen(url, exchangeName, queueName, queueOptions, callback) {
      return amqplib.connect(url)
        .then(function (conn) {
          return conn.createChannel();
        })
        .then(Worker.createConsume(log, exchangeName, queueName, queueOptions));
    }
  });
}

Worker.createConsume = createConsume;

/**
 * Returns a promise for a function.
 * The resoled function, consume, takes two parameters: callback and errback.
 * The callback must take a JavaScript object deserialized from an AMQP message body
 * and return a promise for an object to be serialized for the response.
 * The errback must take a reason and a JavaScript error. It should return an object
 * to be serialized describing the error. The errback is called when the the
 * callbacks promise is rejected or when a synchronous exception is caught.
 *
 * @param {Object} log
 * @param {String} exchangeName
 * @param {String} queueName
 * @param {Object} queueOptions
 * @returns {Promise}  A promise that resolves to a function that takes a callback and an errback.
 */
function createConsume(log, exchangeName, queueName, queueOptions) {
  return function (channel) {
    return channel.assertQueue(queueName, queueOptions)
      .then(function () {
        return channel.bindQueue(queueName, exchangeName, queueName);
      })
      .tap(function () {
        log.info('[AmqpWorker] asserted queue %s', queueName);
      })
      .then(function () {
        /**
         * Creates a consumer that expects message body to be
         * a JavaScript object serialized as JSON. It deserializes
         * the object and passes it to @paramref callback.
         * The callback must return a Promise. If the promise resolves,
         * the resulting JavaScript object is serialized and sent to the
         * reply_to address. If there is an error, the error message and 
         * a stack trace are sent to the reply_to address.
         * 
         * @param {Function} callback  Gets a JSON Object as an argument. Must return a Promise.
         * @param {Function} errback   Takes two parameters: reason and stack. Returns an object
         *    representing the error.
         */
        return function consume(callback, errback) {
          channel.consume(queueName, reply(channel, callback, errback || formatError, log));
          log.info('[AmqpWorker] attached callback as consumer on %s', queueName);
        };
      });
  };
}

/**
 * Creates a message handler for a channel.
 * Expects messages with JSON bodies to be deserialized. The parsed object
 * is passed to the callback. The callback must return a promise.
 * The promises resolution is serialized to JSON and 
 * sent to the reply_to address specified in the
 * request's headers.
 *
 * @param {Object} channel  AMQP Channel
 * @param {Function} callback  A function that takes a JavaScript object
 *    representing the request and returns a Promise. If the promise resolves,
 *    this message is sent to the reply_to address specified in the AMQP headers.
 * @returns {Function}
 */
function reply(channel, callback, formatError, log) {
  return function (msg) {
    log.info('received msg', _.omit(msg, 'content'));

    var req, replyTo;

    try {
      replyTo = getReplyTo(msg).replace('direct:///', '');
    } catch (err) {
      log.error('[AmqpWorker] no reply to address found %s %s', err, JSON.stringify(msg.properties));

      channel.ack(msg);
      return;
    }

    try {
      req = JSON.parse(msg.content.toString());
    } catch (err) {
      log.error('[AmqpWorker] failed to parse json %s %s', err, msg.content.toString());

      channel.sendToQueue(replyTo, errorRes(formatError('Invalid JSON', err)));

      channel.ack(msg);

      return;
    }

    return Promise
      .try(function () {
        return callback(req);
      })
      .then(function sendSuccessfulResponseToQueue(result) {
        var strResult = JSON.stringify(result);
        log.debug('[AmqpWorker] sending reply to %s. size is %s.', replyTo, strResult.length);
        
        if (strResult.length > 2048) {
          log.debug('[AmqpWorker] first 2k of results is %s', strResult.substring(1, 2048));
        } else {
          log.debug('[AmqpWorker] result is %s', strResult);
        }
        
        channel.sendToQueue(replyTo, new Buffer(strResult), msg.properties);

        return msg;
      })
      .catch(function (err) {
        log.error('[AmqpWorker] list count failure %s %s', err.message, err.stack);
        channel.sendToQueue(replyTo, errorRes(formatError(err.message, err)), msg.properties);
      })
      .finally(function () {
        channel.ack(msg);
      });
  };

  function errorRes(obj) {
    return new Buffer(JSON.stringify(obj));
  }

  function getReplyTo(msg) {
    if (msg.properties.headers && msg.properties.headers.reply_to) {
      return msg.properties.headers.reply_to;
    }

    return msg.properties.reply_to || msg.properties.replyTo;
  }
}

function formatError(reason, err) {
  return {
    reason: reason,
    err: err
  };
}
