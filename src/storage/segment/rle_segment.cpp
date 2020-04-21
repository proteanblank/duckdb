#include "duckdb/storage/segment/rle_segment.hpp"

static RLESegment::append_function_t GetRLEAppendFunction(TypeId type);

// static RLESegment::update_function_t GetUpdateFunction(TypeId type);
//
// static RLESegment::update_info_fetch_function_t GetUpdateInfoFetchFunction(TypeId type);
//
// static RLESegment::rollback_update_function_t GetRollbackUpdateFunction(TypeId type);
//
// static RLESegment::merge_update_function_t GetMergeUpdateFunction(TypeId type);
//
// static RLESegment::update_info_append_function_t GetUpdateInfoAppendFunction(TypeId type);

RLESegment::RLESegment(BufferManager &manager, TypeId type, idx_t row_start, block_id_t block)
    : UncompressedSegment(manager, type, row_start) {
	// set up the different functions for this type of segment
	this->append_function = GetRLEAppendFunction(type);
	//	this->update_function = GetUpdateFunction(type);
	//	this->fetch_from_update_info = GetUpdateInfoFetchFunction(type);
	//	this->append_from_update_info = GetUpdateInfoAppendFunction(type);
	//	this->rollback_update = GetRollbackUpdateFunction(type);
	//	this->merge_update_function = GetMergeUpdateFunction(type);

	// figure out how many vectors we want to store in this block
	this->type_size = GetTypeIdSize(type);
	this->vector_size = sizeof(nullmask_t) + type_size * STANDARD_VECTOR_SIZE;
	this->max_vector_count = Storage::BLOCK_SIZE / vector_size;

	this->block_id = block;
	if (block_id == INVALID_BLOCK) {
		// no block id specified: allocate a buffer for the uncompressed segment
		auto handle = manager.Allocate(Storage::BLOCK_ALLOC_SIZE);
		this->block_id = handle->block_id;
		// initialize nullmasks to 0 for all vectors
		for (idx_t i = 0; i < max_vector_count; i++) {
			auto mask = (nullmask_t *)(handle->node->buffer + (i * vector_size));
			mask->reset();
		}
	}
}
void RLESegment::FetchUpdateData(ColumnScanState &state, Transaction &transaction, UpdateInfo *versions,
                                 Vector &result) {
	assert(0);
}
void RLESegment::FilterFetchBaseData(ColumnScanState &state, Vector &result, SelectionVector &sel,
                                     idx_t &approved_tuple_count) {
	assert(0);
}
void RLESegment::FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) {
	assert(0);
}
void RLESegment::Select(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count,
                        vector<TableFilter> &tableFilter) {
	assert(0);
}
void RLESegment::Update(ColumnData &data, SegmentStatistics &stats, Transaction &transaction, Vector &update,
                        row_t *ids, idx_t count, idx_t vector_index, idx_t vector_offset, UpdateInfo *node) {
	assert(0);
}
void RLESegment::RollbackUpdate(UpdateInfo *info) {
	assert(0);
}
idx_t RLESegment::Append(SegmentStatistics &stats, Vector &data, idx_t offset, idx_t count) {
	assert(0);
	return 0;
}
void RLESegment::FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
                          idx_t result_idx) {
	assert(0);
}
template <class T>
static void append_loop_rle(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, Vector &source,
                            idx_t offset, idx_t count) {
	auto &nullmask = *((nullmask_t *)target);
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();

	VectorData adata;
	source.Orrify(count, adata);

	auto sdata = (T *)adata.data;
	auto tdata = (T *)(target + sizeof(nullmask_t));
	if (adata.nullmask->any()) {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			bool is_null = (*adata.nullmask)[source_idx];
			if (is_null) {
				nullmask[target_idx] = true;
				stats.has_null = true;
			} else {
				update_min_max(sdata[source_idx], min, max);
				tdata[target_idx] = sdata[source_idx];
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			update_min_max(sdata[source_idx], min, max);
			tdata[target_idx] = sdata[source_idx];
		}
	}
}

static RLESegment::append_function_t GetRLEAppendFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return append_loop_rle<int8_t>;
	case TypeId::INT16:
		return append_loop_rle<int16_t>;
	case TypeId::INT32:
		return append_loop_rle<int32_t>;
	case TypeId::INT64:
		return append_loop_rle<int64_t>;
	case TypeId::FLOAT:
		return append_loop_rle<float>;
	case TypeId::DOUBLE:
		return append_loop_rle<double>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}
