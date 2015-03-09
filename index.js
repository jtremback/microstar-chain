'use strict';

var mMessage = require('../microstar-message')
var llibrarian = require('../level-librarian')
var pull = require('pull-stream')

module.exports = {
  read: read,
  readOne: llibrarian.makeReadOne(read),
  write: write,
  writeOne: llibrarian.makeWriteOne(write),
  copyOne: llibrarian.makeWriteOne(copy),
  copy: copy,
  createDocs: createDocs,
  sequential: sequential,
  index_defs: [
    ['public_key', 'chain_id', 'sequence']
  ]
}

// settings = {
//   crypto: JS,
//   keys: JS,
//   db: db
// }

// message = {
//   content: JSON,
//   type: String,
//   chain_id: String
// }

function createEnvelopes (settings) {
  return processMessages(settings, mMessage.createEnvelope, settings.keys.public_key)
}

function validate (settings) {
  return processMessages(settings, mMessage.validate)
}

function processMessages (settings, func, public_key) {
  var previous
  return pull.asyncMap(function (message, callback) {
    if (!previous) {
      // Get previous message from db
      llibrarian.readOne(settings, {
        k: ['public_key', 'chain_id', 'sequence'],
        v: [(public_key || message.public_key), message.chain_id],
        peek: 'last'
      }, function (err, prev) {
        // Now that we have the previous message...
        if (prev) { prev = prev.value }
        // Create envelope for message and callback
        func(settings, message, prev, function (err, message) {
          // Set previous
          previous = message
          return callback(err, message)
        })
      })
    } else {
      // Create envelope for message and callback
      func(settings, message, previous, function (err, message) {
        // Set previous
        previous = message
        return callback(err, message)
      })
    }
  })
}

// Formats messages and then writes them to db
function write (settings, callback) {
  return pull(
    createEnvelopes(settings),
    createDocs(settings),
    llibrarian.write(settings, callback)
  )
}

// Saves messages without making envelopes, but with validation
// against past messages
function copy (settings, callback) {
  return pull(
    validate(settings),
    createDocs(settings),
    llibrarian.write(settings, callback)
  )
}

// function validate (settings, initial) {
//   var previous
//   return pull.asyncMap(function (message, callback) {
//     mMessage.validate(settings, message, previous || initial, function (err, message) {
//       previous = message
//       callback(err, message)
//     })
//   })
// }

function createDocs (settings) {
  return pull.asyncMap(function (message, callback) {
    mMessage.createDoc(settings, message, callback)
  })
}

function read (settings, query) {
  return pull(
    llibrarian.read(settings, query),
    pull.map(function (item) {
      // Get out of doc format
      return item.value
    })
  )
}

// This outputs a sequential stream of messages suitable for serialization
// or validation.
function sequential (settings, public_key, chain_id, sequence) {
  var query
  if (sequence) {
    query = {
      k: ['public_key', 'chain_id', 'sequence'],
      v: [public_key, chain_id, [sequence, null]]
    }
  } else { // If no sequence arg, get all
    query = {
      k: ['public_key', 'chain_id', 'sequence'],
      v: [public_key, chain_id]
    }
  }

  return read(settings, query)
}
