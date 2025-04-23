//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/table_data_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

struct TableDataInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::TABLE_DATA_INFO;

public:
	TableDataInfo() : ParseInfo(TYPE) {
	}

	//! Schema of the table
	string schema;
	//! Name of the table
	string name;

public:
	unique_ptr<TableDataInfo> Copy() const {
		auto result = make_uniq<TableDataInfo>();
		result->schema = schema;
		result->name = name;
		return result;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

struct InsertInfo : public TableDataInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::INSERT_INFO;

public:
	InsertInfo() : TableDataInfo() {
	}

	//! The chunk to insert
	unique_ptr<DataChunk> chunk;

public:
	unique_ptr<InsertInfo> Copy() const {
		auto result = make_uniq<InsertInfo>();
		result->schema = schema;
		result->name = name;
		if (chunk) {
			result->chunk = make_uniq<DataChunk>();
			result->chunk->Copy(*chunk);
		}
		return result;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

struct DeleteInfo : public TableDataInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::DELETE_INFO;

public:
	DeleteInfo() : TableDataInfo() {
	}

	//! The row IDs to delete
	unique_ptr<DataChunk> chunk;

public:
	unique_ptr<DeleteInfo> Copy() const {
		auto result = make_uniq<DeleteInfo>();
		result->schema = schema;
		result->name = name;
		if (chunk) {
			result->chunk = make_uniq<DataChunk>();
			result->chunk->Copy(*chunk);
		}
		return result;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

struct UpdateInfo : public TableDataInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::UPDATE_INFO;

public:
	UpdateInfo() : TableDataInfo() {
	}

	//! The column indexes to update
	vector<column_t> column_indexes;
	//! The update chunk (including row IDs at the end)
	unique_ptr<DataChunk> chunk;

public:
	unique_ptr<UpdateInfo> Copy() const {
		auto result = make_uniq<UpdateInfo>();
		result->schema = schema;
		result->name = name;
		result->column_indexes = column_indexes;
		if (chunk) {
			result->chunk = make_uniq<DataChunk>();
			result->chunk->Copy(*chunk);
		}
		return result;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

struct RowGroupDataInfo : public TableDataInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::ROW_GROUP_DATA_INFO;

public:
	RowGroupDataInfo() : TableDataInfo() {
	}

	//! The persistent collection data containing row groups
	PersistentCollectionData collection_data;

public:
	unique_ptr<RowGroupDataInfo> Copy() const {
		auto result = make_uniq<RowGroupDataInfo>();
		result->schema = schema;
		result->name = name;
		// collection_data can't be copied
		return result;
	}

	void Serialize(Serializer &serializer) const override {
		TableDataInfo::Serialize(serializer);
		serializer.WriteProperty(101, "row_group_data", collection_data);
	}

	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer) {
		auto result = make_uniq<RowGroupDataInfo>();
		auto base = TableDataInfo::Deserialize(deserializer);
		auto &info = base->Cast<TableDataInfo>();
		result->schema = info.schema;
		result->name = info.name;
		deserializer.ReadProperty(101, "row_group_data", result->collection_data);
		return std::move(result);
	}
};

} // namespace duckdb
