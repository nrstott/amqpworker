'use strict';

var assert = require('assert');
var AmqpListener = require('../lib/amqpworker');
var Promise = require('bluebird');
var sinon = require('sinon');

describe('amqplistener', function () {

  describe('listen', function (done) {
    var QUEUE_NAME    = 'myqueue',
        EXCHANGE_NAME = 'myexchange';

    var amqplib, conn, channel, callback;

    beforeEach(function (done) {
      channel = {
        assertQueue: sinon.stub().returns(Promise.resolve()),
        bindQueue: sinon.stub().returns(Promise.resolve()),
        consume: sinon.spy(),
        sendToQueue: sinon.spy(),
        ack: sinon.spy()
      };

      conn = {
        createChannel: sinon.stub().returns(Promise.resolve(channel))
      };

      amqplib = {
        connect: sinon.stub().returns(Promise.resolve(conn))
      };

      callback = sinon.stub().returns(Promise.resolve(JSON.stringify({hello:'there'})));

      new AmqpListener(amqplib).listen('http://test.com', EXCHANGE_NAME, QUEUE_NAME, { durable: true })
        .then(function (consume) { consume(callback); })
        .finally(done);
    });

    it('should assert queue', function () {
      assert(channel.assertQueue.calledWith(QUEUE_NAME));
    });

    it('should bind queue', function () {
      assert(channel.bindQueue.calledWith(QUEUE_NAME, EXCHANGE_NAME, QUEUE_NAME));
    });

    it('should consume', function () {
      assert(channel.consume.calledWith(QUEUE_NAME));
    });

    describe('consumer', function () {
      var msg;

      beforeEach(function (done) {
        msg = {
          hello: 'world',
          content: new Buffer(JSON.stringify({hello:'there'})),
          properties: {
            headers: { reply_to: 'abcdef' }
          }
        };

        channel.consume.args[0][1](msg).finally(done);
      });

      it('should call callback', function () {
        assert(callback.called);
      });

      it('should send to reply queue', function () {
        assert(channel.sendToQueue.calledWith(msg.properties.headers.reply_to));
      });
    });
  });

});
