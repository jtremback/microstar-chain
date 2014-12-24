'use strict';

var mMessage = require('microstar-message')
var llibrarian = require('level-librarian')
var pairs = require('pull-pairs')
var pull = require('pull-stream')
var r = require('ramda')

module.exports = function (settings) {
  return {
    read: llibrarian.read.bind(null, settings),
    write: write.bind(null, settings),
    copy: copy.bind(null, settings)
  }
}

module.exports.read = llibrarian.read
module.exports.write = write
module.exports.copy = copy

module.exports.indexes = [
  ['pub_key', 'chain_id', 'sequence']
]

// settings = {
//   crypto: JS,
//   keys: JS,
//   db: db
// }

// message = {
//   content: JSON,
//   type: String,
//   feed_id: String
// }

// Formats messages and then writes them to db
function write (settings, callback) {
  return pull(
    formatMessages(settings),
    llibrarian.write(settings, callback)
  )
}

// Saves messages without formatting, but with validation
// against past messages
function copy (settings, initial, level_opts, callback) {
  return pull(
    validateMessages(settings, initial),
    llibrarian.write(settings, callback)
  )
}

// This will stop a stream on the first invalid message
function validateMessages (settings, initial) {
  return pull(
    pairs(function (a, b) {
      return [a, b]
    }),
    pull.mapAsync(function (pair, callback) {
      // If a is null, use supplied initial message
      mMessage.validate(settings, pair[1], (pair[0] || initial), function (err) {
        if (err) { return callback(err) }
        else {
          return callback(null, pair[1])
        }
      })
    }),
    // Stop stream on invalid message
    pull.take(r.identity)
  )
}

function formatMessages (settings) {
  return pull(
    pull.asyncMap(function (message, cb) {
      formatMessage(settings, message, cb)
    })
  )
}

// Gets previous message and creates a new message in the right format
function formatMessage (settings, message, callback) {
  // Get previous message from db
  llibrarian.readOne(settings, {
    k: ['feed_id', '$latest'],
    v: message.feed_id
  }, function (err, prev) {
    mMessage.create(settings, message, prev, callback)
  })
}