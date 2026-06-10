// Package pb holds the protobuf wire messages for the slot simulator.
package pb

//go:generate protoc --go_out=. --go_opt=paths=source_relative block.proto attestation.proto aggregate.proto column.proto sync.proto decoupled.proto partial.proto
