//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/expression_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

//! A column reader that executes an expression over a child reader
class ExpressionColumnReader : public ColumnReader {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::INVALID;

public:
	ExpressionColumnReader(ClientContext &context, unique_ptr<ColumnReader> child_reader, unique_ptr<Expression> expr,
	                       const ParquetColumnSchema &schema);
	ExpressionColumnReader(ClientContext &context, unique_ptr<ColumnReader> child_reader, unique_ptr<Expression> expr,
	                       unique_ptr<ParquetColumnSchema> owned_schema);

	unique_ptr<ColumnReader> child_reader;
	DataChunk intermediate_chunk;
	unique_ptr<Expression> expr;
	ExpressionExecutor executor;

	// If this reader was created on top of a child reader, after-the-fact, the schema needs to live somewhere
	unique_ptr<ParquetColumnSchema> owned_schema;

public:
	void InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns, TProtocol &protocol_p) override;

	idx_t Read(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) override;

	void Skip(idx_t num_values) override;
	idx_t GroupRowsAvailable() override;

	uint64_t TotalCompressedSize() override {
		return child_reader->TotalCompressedSize();
	}

	idx_t FileOffset() const override {
		return child_reader->FileOffset();
	}

	void RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) override {
		child_reader->RegisterPrefetch(transport, allow_merge);
	}
};

} // namespace duckdb
