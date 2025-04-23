//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/sequence_value_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct SequenceValueInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::SEQUENCE_VALUE_INFO;

public:
	SequenceValueInfo() : ParseInfo(TYPE) {};

	//! Schema name
	string schema;
	//! Sequence name
	string name;
	//! Usage count
	uint64_t usage_count;
	//! Counter value
	int64_t counter;

public:
	unique_ptr<SequenceValueInfo> Copy() const {
		auto result = make_uniq<SequenceValueInfo>();
		result->schema = schema;
		result->name = name;
		result->usage_count = usage_count;
		result->counter = counter;
		return result;
	}

	void Serialize(Serializer &serializer) const override {
		serializer.WriteProperty(100, "schema", schema);
		serializer.WriteProperty(101, "name", name);
		serializer.WriteProperty(102, "usage_count", usage_count);
		serializer.WriteProperty(103, "counter", counter);
	}

	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer) {
		auto result = make_uniq<SequenceValueInfo>();
		result->schema = deserializer.ReadProperty<string>(100, "schema");
		result->name = deserializer.ReadProperty<string>(101, "name");
		result->usage_count = deserializer.ReadProperty<uint64_t>(102, "usage_count");
		result->counter = deserializer.ReadProperty<int64_t>(103, "counter");
		return std::move(result);
	}
};

} // namespace duckdb