//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/wal_write_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {
class CatalogEntry;
class DataChunk;
class DuckTransaction;
class WriteAheadLog;
class ClientContext;
class TableCatalogEntry;

struct DataTableInfo;
struct DeleteInfo;
struct UpdateInfo;

struct PrimaryKeyInfo {
	bool has_primary_key = false;
	vector<idx_t> column_indices;
	vector<string> column_names;
	vector<LogicalType> column_types;
	optional_ptr<TableCatalogEntry> table_entry;

	PrimaryKeyInfo() {
	}
};

class WALWriteState {
public:
	explicit WALWriteState(DuckTransaction &transaction, WriteAheadLog &log,
	                       optional_ptr<StorageCommitState> commit_state);

public:
	void CommitEntry(UndoFlags type, data_ptr_t data);

private:
	void SwitchTable(DataTableInfo *table, UndoFlags new_op);

	void WriteCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data);
	void WriteDelete(DeleteInfo &info);
	void WriteUpdate(UpdateInfo &info);

	// Helper functions for primary key handling
	PrimaryKeyInfo GetPrimaryKeyInfo(DataTableInfo &table_info, optional_ptr<DataTable> data_table = nullptr);
	bool FetchPrimaryKeyValues(PrimaryKeyInfo &pk_info, DataTable &data_table, const vector<row_t> &row_ids,
	                           DataChunk &target_chunk, idx_t start_column_idx);
	bool FetchPrimaryKeyValuesFromTableEntry(PrimaryKeyInfo &pk_info, DataTableInfo &table_info,
	                                         const vector<row_t> &row_ids, DataChunk &target_chunk,
	                                         idx_t start_column_idx);

private:
	DuckTransaction &transaction;
	WriteAheadLog &log;
	optional_ptr<StorageCommitState> commit_state;

	optional_ptr<DataTableInfo> current_table_info;

	unique_ptr<DataChunk> delete_chunk;
	unique_ptr<DataChunk> update_chunk;
};

} // namespace duckdb
