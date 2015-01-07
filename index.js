'use strict';

var mMessage = require('../microstar-message')
var llibrarian = require('../level-librarian')
var pairs = require('pull-pairs')
var pull = require('pull-stream')

module.exports = function (settings) {
  return {
    read: llibrarian.read.bind(null, settings),
    write: write.bind(null, settings),
    copy: copy.bind(null, settings),
    validate: validate.bind(null, settings),
    format: format.bind(null, settings),
    sequential: sequential.bind(null, settings)
  }
}

module.exports.read = llibrarian.read
module.exports.write = write
module.exports.copy = copy
module.exports.validate = validate
module.exports.format = format
module.exports.sequential = sequential

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
//   chain_id: String
// }

// Formats messages and then writes them to db
function write (settings, callback) {
  return pull(
    format(settings),
    llibrarian.write(settings, callback)
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
