'use strict';

var mMessage = require('microstar-message')
var llibrarian = require('level-librarian')
var pairs = require('pull-pairs')
var pull = require('pull-stream')
var r = require('ramda')

module.exports = function (settings) {
  return {
    write: write.bind(null, settings),
    read: read.bind(null, settings),
    save: save.bind(null, settings)
  }
}

module.exports.write = write
module.exports.read = read

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
function write (settings, level_opts, callback) {
  return pull(
    formatMessages(settings),
    llibrarian.write(settings.db, settings.indexes, level_opts, callback)
  )
}

// Just exposes normal read functionality
function read (settings, query, callback) {
  return pull(
    llibrarian.read(settings.db, query, callback)
  )
}

// Saves messages without formatting, but with validation
// against past messages
function save (settings, initial, level_opts, callback) {
  return pull(
    validateMessages(settings, initial),
    llibrarian.write(settings.db, settings.indexes, level_opts, callback)
  )
}

// This will stop a stream on the first invalid message
function validateMessages (settings, initial) {
  return pull(
    pairs.async(function (a, b, callback) {
      // If a is null, use supplied initial message
      mMessage.validate(settings, b, (a || initial), function (err) {
        if (err) { return callback(err) }
        else {
          return callback(null, b)
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
  llibrarian.readOne(settings.db, {
    k: ['feed_id', '$latest'],
    v: message.feed_id
  }, function (err, prev) {
    mMessage.create(settings, message, prev, callback)
  })
}