#include "duckdb/transaction/wal_write_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/transaction/append_info.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/update_info.hpp"

namespace duckdb {

WALWriteState::WALWriteState(DuckTransaction &transaction_p, WriteAheadLog &log,
                             optional_ptr<StorageCommitState> commit_state)
    : transaction(transaction_p), log(log), commit_state(commit_state), current_table_info(nullptr) {
}

void WALWriteState::SwitchTable(DataTableInfo *table_info, UndoFlags new_op) {
	if (current_table_info != table_info) {
		// write the current table to the log
		log.WriteSetTable(table_info->GetSchemaName(), table_info->GetTableName());
		current_table_info = table_info;
	}
}

void WALWriteState::WriteCatalogEntry(CatalogEntry &entry, data_ptr_t dataptr) {
	if (entry.temporary || entry.Parent().temporary) {
		return;
	}

	// look at the type of the parent entry
	auto &parent = entry.Parent();

	switch (parent.type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
	case CatalogType::INDEX_ENTRY:
	case CatalogType::SEQUENCE_ENTRY:
	case CatalogType::TYPE_ENTRY:
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
		if (entry.type == CatalogType::RENAMED_ENTRY || entry.type == parent.type) {
			// ALTER statement, read the extra data after the entry
			auto extra_data_size = Load<idx_t>(dataptr);
			auto extra_data = data_ptr_cast(dataptr + sizeof(idx_t));

			MemoryStream source(extra_data, extra_data_size);
			BinaryDeserializer deserializer(source);
			deserializer.Begin();
			auto column_name = deserializer.ReadProperty<string>(100, "column_name");
			auto parse_info = deserializer.ReadProperty<unique_ptr<ParseInfo>>(101, "alter_info");
			deserializer.End();

			auto &alter_info = parse_info->Cast<AlterInfo>();
			log.WriteAlter(entry, alter_info);
		} else {
			switch (parent.type) {
			case CatalogType::TABLE_ENTRY:
				// CREATE TABLE statement
				log.WriteCreateTable(parent.Cast<TableCatalogEntry>());
				break;
			case CatalogType::VIEW_ENTRY:
				// CREATE VIEW statement
				log.WriteCreateView(parent.Cast<ViewCatalogEntry>());
				break;
			case CatalogType::INDEX_ENTRY:
				// CREATE INDEX statement
				log.WriteCreateIndex(parent.Cast<IndexCatalogEntry>());
				break;
			case CatalogType::SEQUENCE_ENTRY:
				// CREATE SEQUENCE statement
				log.WriteCreateSequence(parent.Cast<SequenceCatalogEntry>());
				break;
			case CatalogType::TYPE_ENTRY:
				// CREATE TYPE statement
				log.WriteCreateType(parent.Cast<TypeCatalogEntry>());
				break;
			case CatalogType::MACRO_ENTRY:
				log.WriteCreateMacro(parent.Cast<ScalarMacroCatalogEntry>());
				break;
			case CatalogType::TABLE_MACRO_ENTRY:
				log.WriteCreateTableMacro(parent.Cast<TableMacroCatalogEntry>());
				break;
			default:
				throw InternalException("Don't know how to create this type!");
			}
		}
		break;
	case CatalogType::SCHEMA_ENTRY:
		if (entry.type == CatalogType::RENAMED_ENTRY || entry.type == CatalogType::SCHEMA_ENTRY) {
			// ALTER TABLE statement, skip it
			return;
		}
		log.WriteCreateSchema(parent.Cast<SchemaCatalogEntry>());
		break;
	case CatalogType::RENAMED_ENTRY:
		// This is a rename, nothing needs to be done for this
		break;
	case CatalogType::DELETED_ENTRY:
		switch (entry.type) {
		case CatalogType::TABLE_ENTRY: {
			auto &table_entry = entry.Cast<DuckTableEntry>();
			D_ASSERT(table_entry.IsDuckTable());
			log.WriteDropTable(table_entry);
			break;
		}
		case CatalogType::SCHEMA_ENTRY:
			log.WriteDropSchema(entry.Cast<SchemaCatalogEntry>());
			break;
		case CatalogType::VIEW_ENTRY:
			log.WriteDropView(entry.Cast<ViewCatalogEntry>());
			break;
		case CatalogType::SEQUENCE_ENTRY:
			log.WriteDropSequence(entry.Cast<SequenceCatalogEntry>());
			break;
		case CatalogType::MACRO_ENTRY:
			log.WriteDropMacro(entry.Cast<ScalarMacroCatalogEntry>());
			break;
		case CatalogType::TABLE_MACRO_ENTRY:
			log.WriteDropTableMacro(entry.Cast<TableMacroCatalogEntry>());
			break;
		case CatalogType::TYPE_ENTRY:
			log.WriteDropType(entry.Cast<TypeCatalogEntry>());
			break;
		case CatalogType::INDEX_ENTRY: {
			log.WriteDropIndex(entry.Cast<IndexCatalogEntry>());
			break;
		}
		case CatalogType::RENAMED_ENTRY:
		case CatalogType::PREPARED_STATEMENT:
		case CatalogType::SCALAR_FUNCTION_ENTRY:
		case CatalogType::DEPENDENCY_ENTRY:
		case CatalogType::SECRET_ENTRY:
		case CatalogType::SECRET_TYPE_ENTRY:
		case CatalogType::SECRET_FUNCTION_ENTRY:
			// do nothing, prepared statements and scalar functions aren't persisted to disk
			break;
		default:
			throw InternalException("Don't know how to drop this type!");
		}
		break;
	case CatalogType::PREPARED_STATEMENT:
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
	case CatalogType::TABLE_FUNCTION_ENTRY:
	case CatalogType::COPY_FUNCTION_ENTRY:
	case CatalogType::PRAGMA_FUNCTION_ENTRY:
	case CatalogType::COLLATION_ENTRY:
	case CatalogType::DEPENDENCY_ENTRY:
	case CatalogType::SECRET_ENTRY:
	case CatalogType::SECRET_TYPE_ENTRY:
	case CatalogType::SECRET_FUNCTION_ENTRY:
		// do nothing, these entries are not persisted to disk
		break;
	default:
		throw InternalException("UndoBuffer - don't know how to write this entry to the WAL");
	}
}

void WALWriteState::WriteDelete(DeleteInfo &info) {
	// switch to the current table, if necessary
	SwitchTable(info.table->GetDataTableInfo().get(), UndoFlags::DELETE_TUPLE);

	// check if replication is enabled - if not, skip primary key operations
	auto &storage_manager = transaction.manager.GetDB().GetStorageManager();
	bool replication_enabled = storage_manager.IsReplicationEnabled();

	// get primary key information only if replication is enabled
	PrimaryKeyInfo pk_info;
	if (replication_enabled) {
		pk_info = GetPrimaryKeyInfo(*info.table->GetDataTableInfo(), info.table);
	}

	// prepare delete chunk types - primary key types + row_type
	vector<LogicalType> delete_types;
	if (replication_enabled) {
		for (const auto &pk_type : pk_info.column_types) {
			delete_types.push_back(pk_type);
		}
	}
	delete_types.push_back(LogicalType::ROW_TYPE);

	if (!delete_chunk) {
		delete_chunk = make_uniq<DataChunk>();
		delete_chunk->Initialize(Allocator::DefaultAllocator(), delete_types);
	} else if (delete_chunk->ColumnCount() != delete_types.size()) {
		// Recreate chunk if column count doesn't match (different table with different PK structure)
		delete_chunk = make_uniq<DataChunk>();
		delete_chunk->Initialize(Allocator::DefaultAllocator(), delete_types);
	}

	auto rows = FlatVector::GetData<row_t>(delete_chunk->data[delete_chunk->ColumnCount() - 1]);
	vector<row_t> deleted_row_ids;

	if (info.is_consecutive) {
		for (idx_t i = 0; i < info.count; i++) {
			auto row_id = UnsafeNumericCast<int64_t>(info.base_row + i);
			rows[i] = row_id;
			deleted_row_ids.push_back(row_id);
		}
	} else {
		auto delete_rows = info.GetRows();
		for (idx_t i = 0; i < info.count; i++) {
			auto row_id = UnsafeNumericCast<int64_t>(info.base_row) + delete_rows[i];
			rows[i] = row_id;
			deleted_row_ids.push_back(row_id);
		}
	}

	delete_chunk->SetCardinality(info.count);

	// fetch primary key values for the deleted rows only if replication is enabled
	if (replication_enabled && !pk_info.column_names.empty()) {
		FetchPrimaryKeyValues(pk_info, *info.table, deleted_row_ids, *delete_chunk, 0);
	}

	log.WriteDelete(*delete_chunk);
}

void WALWriteState::WriteUpdate(UpdateInfo &info) {
	// switch to the current table, if necessary
	auto &column_data = info.segment->column_data;
	auto &table_info = column_data.GetTableInfo();

	SwitchTable(&table_info, UndoFlags::UPDATE_TUPLE);

	// check if replication is enabled - if not, skip primary key operations
	auto &storage_manager = transaction.manager.GetDB().GetStorageManager();
	bool replication_enabled = storage_manager.IsReplicationEnabled();

	// get primary key information only if replication is enabled
	PrimaryKeyInfo pk_info;
	if (replication_enabled) {
		pk_info = GetPrimaryKeyInfo(table_info, nullptr);
	}

	// initialize the update chunk
	vector<LogicalType> update_types;
	if (column_data.type.id() == LogicalTypeId::VALIDITY) {
		update_types.emplace_back(LogicalType::BOOLEAN);
	} else {
		update_types.push_back(column_data.type);
	}
	update_types.emplace_back(LogicalType::ROW_TYPE);

	// add primary key column types to the update chunk only if replication is enabled
	if (replication_enabled) {
		for (const auto &pk_type : pk_info.column_types) {
			update_types.push_back(pk_type);
		}
	}

	update_chunk = make_uniq<DataChunk>();
	update_chunk->Initialize(Allocator::DefaultAllocator(), update_types);

	// fetch the updated values from the base segment
	info.segment->FetchCommitted(info.vector_index, update_chunk->data[0]);

	// write the row ids into the chunk
	auto row_ids = FlatVector::GetData<row_t>(update_chunk->data[1]);
	idx_t start = column_data.start + info.vector_index * STANDARD_VECTOR_SIZE;
	auto tuples = info.GetTuples();
	for (idx_t i = 0; i < info.N; i++) {
		row_ids[tuples[i]] = UnsafeNumericCast<int64_t>(start + tuples[i]);
	}
	if (column_data.type.id() == LogicalTypeId::VALIDITY) {
		// zero-initialize the booleans
		// FIXME: this is only required because of NullValue<T> in Vector::Serialize...
		auto booleans = FlatVector::GetData<bool>(update_chunk->data[0]);
		for (idx_t i = 0; i < info.N; i++) {
			auto idx = tuples[i];
			booleans[idx] = false;
		}
	}

	// fetch primary key values for the updated rows only if replication is enabled
	if (replication_enabled && !pk_info.column_names.empty()) {
		vector<row_t> updated_row_ids;
		for (idx_t i = 0; i < info.N; i++) {
			updated_row_ids.push_back(row_ids[tuples[i]]);
		}
		FetchPrimaryKeyValuesFromTableEntry(pk_info, table_info, updated_row_ids, *update_chunk, 2);
	}

	SelectionVector sel(tuples);
	update_chunk->Slice(sel, info.N);

	// construct the column index path
	vector<column_t> column_indexes;
	reference<const ColumnData> current_column_data = column_data;
	while (current_column_data.get().HasParent()) {
		column_indexes.push_back(current_column_data.get().column_index);
		current_column_data = current_column_data.get().Parent();
	}
	column_indexes.push_back(info.column_index);
	std::reverse(column_indexes.begin(), column_indexes.end());

	log.WriteUpdate(*update_chunk, column_indexes);
}

PrimaryKeyInfo WALWriteState::GetPrimaryKeyInfo(DataTableInfo &table_info, optional_ptr<DataTable> data_table) {
	PrimaryKeyInfo pk_info;

	// If data_table is provided, use the direct index approach (for Delete path)
	if (data_table) {
		string table_name = table_info.GetTableName();
		string schema_name = table_info.GetSchemaName();

		// Access the indexes directly from DataTableInfo to avoid catalog dependency
		// Get the table's index list directly from DataTableInfo
		auto &index_list = table_info.GetIndexes();

		// Look for primary key indexes by scanning the indexes
		index_list.Scan([&](Index &index) {
			// Check if this is a primary key  or unique index
			if (index.GetConstraintType() == IndexConstraintType::PRIMARY ||
			    index.GetConstraintType() == IndexConstraintType::UNIQUE) {
				// Get the column IDs from the index
				auto column_ids = index.GetColumnIds();

				pk_info.has_primary_key = true;
				pk_info.column_indices = column_ids;

				// Get real column names and types from the DataTable
				auto &column_definitions = data_table->Columns();

				for (auto column_id : column_ids) {
					if (column_id < column_definitions.size()) {
						auto &column_def = column_definitions[column_id];
						pk_info.column_names.push_back(column_def.Name());
						pk_info.column_types.push_back(column_def.Type());
					}
				}
				return true; // Stop scanning after finding primary key
			}
			return false; // Continue scanning
		});

		return pk_info;
	} else {
		// Use direct index approach (for Update path) - same as Delete path to avoid catalog access
		table_info.GetIndexes().Scan([&](Index &index) {
			if (index.IsUnique() || index.IsPrimary()) {
				pk_info.has_primary_key = true;

				auto column_ids = index.GetColumnIds();
				pk_info.column_indices = column_ids;

				// Generate generic column names and types instead of requiring real column names
				// This avoids the need to access DataTable.Columns() which could fail when data_table is null
				for (idx_t i = 0; i < column_ids.size(); i++) {
					pk_info.column_names.push_back("pk_col_" + std::to_string(i));
					// For column types, we'll use a generic type that works for primary keys
					// Most primary keys are integers, but we'll use VARCHAR as it's most flexible
					pk_info.column_types.push_back(LogicalType::VARCHAR);
				}
				return true; // Stop scanning after finding primary key
			}
			return false; // Continue scanning
		});

		return pk_info;
	}
}

bool WALWriteState::FetchPrimaryKeyValues(PrimaryKeyInfo &pk_info, DataTable &data_table, const vector<row_t> &row_ids,
                                          DataChunk &target_chunk, idx_t start_column_idx) {

	if (pk_info.column_names.empty() || !pk_info.has_primary_key) {
		return false;
	}

	// Create column IDs vector for primary key columns
	vector<StorageIndex> pk_column_storage_ids;
	for (idx_t pk_idx = 0; pk_idx < pk_info.column_indices.size(); pk_idx++) {
		idx_t column_idx = pk_info.column_indices[pk_idx];
		if (column_idx < data_table.ColumnCount()) {
			auto &column = data_table.Columns()[column_idx];
			pk_column_storage_ids.emplace_back(column.StorageOid());
		}
	}

	if (!pk_column_storage_ids.empty()) {
		// Create a vector of the row IDs
		Vector row_id_vector(LogicalType::ROW_TYPE);
		auto row_id_data = FlatVector::GetData<row_t>(row_id_vector);
		for (idx_t i = 0; i < row_ids.size(); i++) {
			row_id_data[i] = row_ids[i];
		}
		row_id_vector.SetVectorType(VectorType::FLAT_VECTOR);

		// Create a DataChunk to store the fetched primary key values
		DataChunk pk_chunk;
		pk_chunk.Initialize(Allocator::DefaultAllocator(), pk_info.column_types);

		// Fetch primary key values using DataTable's Fetch method which handles row groups internally
		ColumnFetchState fetch_state;
		data_table.Fetch(transaction, pk_chunk, pk_column_storage_ids, row_id_vector, row_ids.size(), fetch_state);

		// Check if we actually fetched the expected number of rows
		if (pk_chunk.size() != row_ids.size()) {
			// The row(s) were not found (likely already deleted in this transaction)

			// Access the database instance through the transaction manager
			auto &attached_db = transaction.manager.GetDB();
			auto &database = attached_db.GetDatabase();
			auto &duck_transaction_manager = DuckTransactionManager::Get(attached_db);

			// Get the ClientContext from the current transaction
			auto context_ptr = transaction.context.lock();
			if (!context_ptr) {
				return false;
			}

			// Create a new transaction with an earlier timestamp to see the deleted row
			auto earlier_start_time = transaction.start_time > 0 ? transaction.start_time - 1 : 0;
			// Use lowest active start as a safe earlier timestamp instead of private GetCommitTimestamp
			auto earlier_transaction_id = duck_transaction_manager.LowestActiveStart();

			// Create a DuckTransaction directly with the earlier timestamp
			auto earlier_transaction =
			    make_uniq<DuckTransaction>(duck_transaction_manager, *context_ptr, earlier_start_time,
			                               earlier_transaction_id, transaction.catalog_version);

			// Reset the chunk and try again with the earlier transaction
			pk_chunk.Reset();
			ColumnFetchState earlier_fetch_state;
			data_table.Fetch(*earlier_transaction, pk_chunk, pk_column_storage_ids, row_id_vector, row_ids.size(),
			                 earlier_fetch_state);

			// Check if the earlier transaction found the rows
			if (pk_chunk.size() != row_ids.size()) {
				return false;
			}
		}

		// Copy the fetched values into the target chunk vectors
		for (idx_t pk_idx = 0; pk_idx < pk_info.column_names.size(); pk_idx++) {
			if (pk_idx < pk_chunk.ColumnCount()) {
				auto &pk_vector = target_chunk.data[start_column_idx + pk_idx];
				VectorOperations::Copy(pk_chunk.data[pk_idx], pk_vector, row_ids.size(), 0, 0);
			}
		}
		return true;
	}

	return false;
}

bool WALWriteState::FetchPrimaryKeyValuesFromTableEntry(PrimaryKeyInfo &pk_info, DataTableInfo &table_info,
                                                        const vector<row_t> &row_ids, DataChunk &target_chunk,
                                                        idx_t start_column_idx) {
	if (pk_info.column_names.empty() || !pk_info.table_entry) {
		return false;
	}

	// Get the storage for this table
	auto &table_storage = pk_info.table_entry->GetStorage();

	// Create column IDs vector for primary key columns
	vector<StorageIndex> pk_column_storage_ids;
	for (idx_t pk_idx = 0; pk_idx < pk_info.column_names.size(); pk_idx++) {
		string column_name = pk_info.column_names[pk_idx];
		auto column_idx = pk_info.table_entry->GetColumnIndex(column_name);
		if (column_idx.IsValid()) {
			auto &column = pk_info.table_entry->GetColumn(column_idx);
			pk_column_storage_ids.emplace_back(column.StorageOid());
		}
	}

	if (!pk_column_storage_ids.empty()) {
		// Create a vector of the row IDs
		Vector row_id_vector(LogicalType::ROW_TYPE);
		auto row_id_data = FlatVector::GetData<row_t>(row_id_vector);
		for (idx_t i = 0; i < row_ids.size(); i++) {
			row_id_data[i] = row_ids[i];
		}
		row_id_vector.SetVectorType(VectorType::FLAT_VECTOR);

		// Create a DataChunk to store the fetched primary key values
		DataChunk pk_chunk;
		pk_chunk.Initialize(Allocator::DefaultAllocator(), pk_info.column_types);

		// Fetch primary key values for the rows
		ColumnFetchState fetch_state;
		table_storage.Fetch(transaction, pk_chunk, pk_column_storage_ids, row_id_vector, row_ids.size(), fetch_state);

		// Check if we actually fetched the expected number of rows
		if (pk_chunk.size() != row_ids.size()) {
			// The row(s) were not found (likely already deleted in this transaction)
			// Return false to indicate primary key fetch failed
			return false;
		}

		// Copy the fetched values into the target chunk vectors
		for (idx_t pk_idx = 0; pk_idx < pk_info.column_names.size(); pk_idx++) {
			if (pk_idx < pk_chunk.ColumnCount()) {
				auto &pk_vector = target_chunk.data[start_column_idx + pk_idx];
				VectorOperations::Copy(pk_chunk.data[pk_idx], pk_vector, row_ids.size(), 0, 0);
			}
		}
		return true;
	}

	return false;
}

void WALWriteState::CommitEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// set the commit timestamp of the catalog entry to the given id
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry->HasParent());
		// push the catalog update to the WAL
		WriteCatalogEntry(*catalog_entry, data + sizeof(CatalogEntry *));
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		// append:
		auto info = reinterpret_cast<AppendInfo *>(data);
		if (!info->table->IsTemporary()) {
			info->table->WriteToLog(transaction, log, info->start_row, info->count, commit_state.get());
		}
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = reinterpret_cast<DeleteInfo *>(data);
		if (!info->table->IsTemporary()) {
			WriteDelete(*info);
		}
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = reinterpret_cast<UpdateInfo *>(data);
		if (!info->segment->column_data.GetTableInfo().IsTemporary()) {
			WriteUpdate(*info);
		}
		break;
	}
	case UndoFlags::SEQUENCE_VALUE: {
		auto info = reinterpret_cast<SequenceValue *>(data);
		log.WriteSequenceValue(*info);
		break;
	}
	default:
		throw InternalException("UndoBuffer - don't know how to commit this type!");
	}
}

} // namespace duckdb
