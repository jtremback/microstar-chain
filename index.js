'use strict';

var mMessage = require('../microstar-message')
var llibrarian = require('../level-librarian')
var pull = require('pull-stream')

module.exports = {
  read: llibrarian.read,
  readOne: llibrarian.readOne,
  write: write,
  writeOne: llibrarian.makeWriteOne(write),
  copy: copy,
  validate: validate,
  format: format,
  sequential: sequential,
  indexes: [
    ['pub_key', 'chain_id', 'sequence']
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

// Formats messages and then writes them to db
function write (settings, callback) {
  return pull(
    format(settings),
    llibrarian.write(settings, callback)
  )
}

// This outputs a sequential stream of messages suitable for serialization
// or validation.
function sequential (settings, pub_key, chain_id, sequence) {
  var query
  if (sequence) {
    query = {
      k: ['pub_key', 'chain_id', 'sequence'],
      v: [pub_key, chain_id, [sequence, null]]
    }
  } else { // If no sequence arg, get all
    query = {
      k: ['pub_key', 'chain_id', 'sequence'],
      v: [pub_key, chain_id]
    }
  }

  return pull(
    llibrarian.read(settings, query),
    // Strip off key for transmission
    pull.map(function (doc) {
      return doc.value
    })
  )
}

// Saves messages without formatting, but with validation
// against past messages
function copy (settings, initial, callback) {
  return pull(
    validate(settings, initial),
    pull.asyncMap(function (message, callback) {
      mMessage.makeDoc(settings, message, callback)
    }),
    llibrarian.write(settings, callback)
  )
}

function format (settings, initial) {
  return formatOrValidate(settings, mMessage.format, initial)
}

function validate (settings, initial) {
  return formatOrValidate(settings, mMessage.validate, initial)
}

// This will error on the first invalid message
function formatOrValidate (settings, op, initial) {
  var last
  return pull.asyncMap(function (message, callback) {
    op(settings, message, last || initial, function (err, message) {
      last = message
      callback(err, message)
    })
  })
}
