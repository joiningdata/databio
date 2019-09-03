// Package formats describes the supported data input/output formats.
// The goal of this package is to support iteration of records (containing fields) within any data set:
//
//       +----------------------------+
//       | Data Set                   |
//       | +------------------------+ |
//       | | Record 1               | |
//       | | Field 1 | Field 2| ... | |
//       | +------------------------+ |
//       | +------------------------+ |
//       | | Record 2               | |
//       | | Field 1 | Field 2| ... | |
//       | +------------------------+ |
//       | +------------------------+ |
//       | | Record 3               | |
//       | | Field 1 | Field 2| ... | |
//       | +------------------------+ |
//       +----------------------------+
//
// Data sets will have multiple records, records can have multiple fields, and fields can have multiple values.
//
package formats
