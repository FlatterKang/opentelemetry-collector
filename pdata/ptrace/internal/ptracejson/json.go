// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ptracejson // import "go.opentelemetry.io/collector/pdata/ptrace/internal/ptracejson"

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/gogo/protobuf/jsonpb"
	jsoniter "github.com/json-iterator/go"

	otlpcollectortrace "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/trace/v1"
	otlpcommon "go.opentelemetry.io/collector/pdata/internal/data/protogen/common/v1"
	otlpresource "go.opentelemetry.io/collector/pdata/internal/data/protogen/resource/v1"
	otlptrace "go.opentelemetry.io/collector/pdata/internal/data/protogen/trace/v1"
	"go.opentelemetry.io/collector/pdata/internal/json"
	"go.opentelemetry.io/collector/pdata/internal/otlp"
)

var JSONMarshaler = &jsonpb.Marshaler{
	// https://github.com/open-telemetry/opentelemetry-specification/pull/2758
	EnumsAsInts: true,
	// https://github.com/open-telemetry/opentelemetry-specification/pull/2829
	OrigName: false,
}

func MarshalTraceData(traceData *otlptrace.TracesData) ([]byte, error) {
	if traceData == nil {
		return nil, nil
	}

	st := jsoniter.ConfigCompatibleWithStandardLibrary.BorrowStream(nil)
	defer jsoniter.ConfigCompatibleWithStandardLibrary.ReturnStream(st)

	st.WriteObjectStart()
	st.WriteObjectField("resourceSpans")
	st.WriteArrayStart()
	for i, spans := range traceData.GetResourceSpans() {
		if err := writeResourceSpans(st, spans); err != nil {
			return nil, err
		}
		if i < len(traceData.GetResourceSpans())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	st.WriteObjectEnd()

	return st.Buffer(), nil
}

func MarshalExportTraceServiceRequest(request *otlpcollectortrace.ExportTraceServiceRequest) ([]byte, error) {
	if request == nil {
		return nil, nil
	}

	st := jsoniter.ConfigCompatibleWithStandardLibrary.BorrowStream(nil)
	defer jsoniter.ConfigCompatibleWithStandardLibrary.ReturnStream(st)

	st.WriteObjectStart()
	st.WriteObjectField("resourceSpans")
	st.WriteArrayStart()
	for i, spans := range request.GetResourceSpans() {
		if err := writeResourceSpans(st, spans); err != nil {
			return nil, err
		}
		if i < len(request.GetResourceSpans())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	st.WriteObjectEnd()

	return st.Buffer(), nil
}

func MarshalExportTraceServiceResponse(request *otlpcollectortrace.ExportTraceServiceResponse) ([]byte, error) {
	panic("implement me")
}

func writeResourceSpans(st *jsoniter.Stream, resourceSpans *otlptrace.ResourceSpans) error {
	st.WriteObjectStart()
	defer st.WriteObjectEnd()

	st.WriteObjectField("deprecatedScopeSpans")
	st.WriteArrayStart()
	for i, spans := range resourceSpans.GetDeprecatedScopeSpans() {
		if err := writeScopeSpans(st, spans); err != nil {
			return err
		}
		if i < len(resourceSpans.GetDeprecatedScopeSpans())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	st.WriteMore()

	st.WriteObjectField("resource")
	if err := writeResource(st, resourceSpans.GetResource()); err != nil {
		return err
	}
	st.WriteMore()

	st.WriteObjectField("scopeSpans")
	st.WriteArrayStart()
	for i, spans := range resourceSpans.GetScopeSpans() {
		if err := writeScopeSpans(st, spans); err != nil {
			return err
		}
		if i < len(resourceSpans.GetScopeSpans())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	st.WriteMore()

	st.WriteObjectField("schemaUrl")
	st.WriteString(resourceSpans.GetSchemaUrl())
	return nil
}

func writeScopeSpans(st *jsoniter.Stream, scopeSpans *otlptrace.ScopeSpans) error {
	if scopeSpans == nil {
		st.WriteNil()
		return nil
	}

	st.WriteObjectStart()
	defer st.WriteObjectEnd()

	scopeSpans.GetScope()

	scopeSpans.GetSpans()
	st.WriteObjectField("spans")
	for i, span := range scopeSpans.GetSpans() {
		if err := writeSpan(st, span); err != nil {
			return err
		}
		if i < len(scopeSpans.GetSpans())-1 {
			st.WriteMore()
		}
	}

	st.WriteObjectField("schemaUrl")
	scopeSpans.GetSchemaUrl()
	return nil
}

func writeSpan(st *jsoniter.Stream, span *otlptrace.Span) error {
	if span == nil {
		st.WriteNil()
		return nil
	}

	st.WriteObjectStart()
	defer st.WriteObjectEnd()

	st.WriteObjectField("traceId")
	st.WriteString(hex.EncodeToString(span.TraceId[:]))
	st.WriteMore()

	st.WriteObjectField("spanId")
	st.WriteString(hex.EncodeToString(span.SpanId[:]))
	st.WriteMore()

	st.WriteObjectField("traceState")
	st.WriteString(span.GetTraceState())
	st.WriteMore()

	st.WriteObjectField("parentSpanId")
	st.WriteString(hex.EncodeToString(span.ParentSpanId[:]))
	st.WriteMore()

	st.WriteObjectField("name")
	st.WriteString(span.GetName())
	st.WriteMore()

	st.WriteObjectField("kind")
	st.WriteInt32(int32(span.GetKind()))
	st.WriteMore()

	st.WriteObjectField("startTimeUnixNano")
	st.WriteUint64(span.GetStartTimeUnixNano())
	st.WriteMore()

	st.WriteObjectField("endTimeUnixNano")
	st.WriteUint64(span.GetEndTimeUnixNano())
	st.WriteMore()

	st.WriteObjectField("attributes")
	st.WriteArrayStart()
	for i, kv := range span.GetAttributes() {
		if err := writeKeyValue(st, kv); err != nil {
			return err
		}
		if i < len(span.GetAttributes())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	st.WriteMore()

	st.WriteObjectField("droppedAttributesCount")
	st.WriteUint32(span.GetDroppedAttributesCount())
	st.WriteMore()

	st.WriteObjectField("events")
	st.WriteArrayStart()
	for i, event := range span.GetEvents() {
		if err := writeSpanEvent(st, event); err != nil {
			return err
		}
		if i < len(span.GetEvents())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	st.WriteMore()

	st.WriteObjectField("droppedEventsCount")
	st.WriteUint32(span.GetDroppedEventsCount())
	st.WriteMore()

	st.WriteObjectField("links")
	st.WriteArrayStart()
	for i, link := range span.GetLinks() {
		if err := writeSpanLink(st, link); err != nil {
			return err
		}
		if i < len(span.GetLinks())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	st.WriteMore()

	st.WriteObjectField("droppedLinksCount")
	st.WriteUint32(span.GetDroppedLinksCount())
	st.WriteMore()

	st.WriteObjectField("status")
	writeStatus(st, span.GetStatus())
	return nil
}

func writeResource(st *jsoniter.Stream, resource otlpresource.Resource) error {
	st.WriteObjectStart()
	defer st.WriteObjectEnd()

	st.WriteObjectField("attributes")
	st.WriteArrayStart()
	for i, kv := range resource.GetAttributes() {
		if err := writeKeyValue(st, kv); err != nil {
			return err
		}
		if i < len(resource.GetAttributes())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	st.WriteMore()

	st.WriteObjectField("droppedAttributesCount")
	st.WriteUint32(resource.GetDroppedAttributesCount())
	return nil
}

func writeKeyValue(st *jsoniter.Stream, kv otlpcommon.KeyValue) error {
	st.WriteObjectStart()
	defer st.WriteObjectEnd()

	st.WriteObjectField("key")
	st.WriteString(kv.GetKey())
	st.WriteMore()

	st.WriteObjectField("value")
	return writeAnyValue(st, kv.GetValue())
}

func writeAnyValue(st *jsoniter.Stream, anyValue otlpcommon.AnyValue) error {
	oneofValue := (&anyValue).GetValue()
	if oneofValue == nil || reflect.ValueOf(oneofValue).IsNil() {
		st.WriteNil()
		return nil
	}

	st.WriteObjectStart()
	defer st.WriteObjectEnd()
	if v, ok := oneofValue.(*otlpcommon.AnyValue_StringValue); ok {
		st.WriteObjectField("stringValue")
		st.WriteString(v.StringValue)
		return nil
	}
	if v, ok := oneofValue.(*otlpcommon.AnyValue_BoolValue); ok {
		st.WriteObjectField("boolValue")
		st.WriteBool(v.BoolValue)
		return nil
	}
	if v, ok := oneofValue.(*otlpcommon.AnyValue_IntValue); ok {
		st.WriteObjectField("intValue")
		st.WriteInt64(v.IntValue)
		return nil
	}
	if v, ok := oneofValue.(*otlpcommon.AnyValue_DoubleValue); ok {
		st.WriteObjectField("doubleValue")
		st.WriteFloat64(v.DoubleValue)
		return nil
	}
	if v, ok := oneofValue.(*otlpcommon.AnyValue_ArrayValue); ok {
		st.WriteObjectField("arrayValue")
		return writeArrayValue(st, v.ArrayValue)
	}
	if v, ok := oneofValue.(*otlpcommon.AnyValue_KvlistValue); ok {
		st.WriteObjectField("kvlistValue")
		return writeKeyValueList(st, v.KvlistValue)
	}
	if v, ok := oneofValue.(*otlpcommon.AnyValue_BytesValue); ok {
		st.WriteObjectField("bytesValue")
		st.WriteString(base64.StdEncoding.EncodeToString(v.BytesValue))
		return nil
	}
	return fmt.Errorf("invalid oneof value type: %v", reflect.TypeOf(oneofValue))
}

func writeArrayValue(st *jsoniter.Stream, arrayValue *otlpcommon.ArrayValue) error {
	if arrayValue == nil {
		st.WriteNil()
		return nil
	}

	st.WriteObjectStart()
	defer st.WriteObjectEnd()
	st.WriteObjectField("values")
	st.WriteArrayStart()
	for i, value := range arrayValue.GetValues() {
		if err := writeAnyValue(st, value); err != nil {
			return err
		}
		if i < len(arrayValue.GetValues())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	return nil
}

func writeKeyValueList(st *jsoniter.Stream, kvlistValue *otlpcommon.KeyValueList) error {
	if kvlistValue == nil {
		st.WriteNil()
		return nil
	}

	st.WriteObjectStart()
	defer st.WriteObjectEnd()
	st.WriteObjectField("values")
	st.WriteArrayStart()
	for i, value := range kvlistValue.GetValues() {
		if err := writeKeyValue(st, value); err != nil {
			return err
		}
		if i < len(kvlistValue.GetValues())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	return nil
}

func writeSpanEvent(st *jsoniter.Stream, event *otlptrace.Span_Event) error {
	if event == nil {
		st.WriteNil()
		return nil
	}

	st.WriteObjectStart()
	defer st.WriteObjectEnd()

	st.WriteObjectField("timeUnixNano")
	st.WriteUint64(event.GetTimeUnixNano())
	st.WriteMore()

	st.WriteObjectField("name")
	st.WriteString(event.GetName())
	st.WriteMore()

	st.WriteObjectField("attributes")
	st.WriteArrayStart()
	for i, kv := range event.GetAttributes() {
		if err := writeKeyValue(st, kv); err != nil {
			return err
		}
		if i < len(event.GetAttributes())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	st.WriteMore()

	st.WriteObjectField("droppedAttributesCount")
	st.WriteUint32(event.GetDroppedAttributesCount())
	return nil
}

func writeStatus(st *jsoniter.Stream, status otlptrace.Status) {
	st.WriteObjectStart()
	defer st.WriteObjectEnd()

	st.WriteObjectField("message")
	st.WriteString(status.GetMessage())
	st.WriteMore()

	st.WriteObjectField("code")
	st.WriteInt32(int32(status.GetCode()))
	return
}

func writeSpanLink(st *jsoniter.Stream, link *otlptrace.Span_Link) error {
	if link == nil {
		st.WriteNil()
		return nil
	}

	st.WriteObjectStart()
	defer st.WriteObjectEnd()

	st.WriteObjectField("traceId")
	st.WriteString(hex.EncodeToString(link.TraceId[:]))
	st.WriteMore()

	st.WriteObjectField("spanId")
	st.WriteString(hex.EncodeToString(link.SpanId[:]))
	st.WriteMore()

	st.WriteObjectField("traceState")
	st.WriteString(link.GetTraceState())
	st.WriteMore()

	st.WriteObjectField("attributes")
	st.WriteArrayStart()
	for i, kv := range link.GetAttributes() {
		if err := writeKeyValue(st, kv); err != nil {
			return err
		}
		if i < len(link.GetAttributes())-1 {
			st.WriteMore()
		}
	}
	st.WriteArrayEnd()
	st.WriteMore()

	st.WriteObjectField("droppedAttributesCount")
	st.WriteUint32(link.GetDroppedAttributesCount())
	return nil
}

func UnmarshalTraceData(buf []byte, dest *otlptrace.TracesData) error {
	iter := jsoniter.ConfigFastest.BorrowIterator(buf)
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "resourceSpans", "resource_spans":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				dest.ResourceSpans = append(dest.ResourceSpans, readResourceSpans(iter))
				return true
			})
		default:
			iter.Skip()
		}
		return true
	})
	otlp.MigrateTraces(dest.ResourceSpans)
	return iter.Error
}

func UnmarshalExportTraceServiceRequest(buf []byte, dest *otlpcollectortrace.ExportTraceServiceRequest) error {
	iter := jsoniter.ConfigFastest.BorrowIterator(buf)
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "resourceSpans", "resource_spans":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				dest.ResourceSpans = append(dest.ResourceSpans, readResourceSpans(iter))
				return true
			})
		default:
			iter.Skip()
		}
		return true
	})
	otlp.MigrateTraces(dest.ResourceSpans)
	return iter.Error
}

func UnmarshalExportTraceServiceResponse(buf []byte, dest *otlpcollectortrace.ExportTraceServiceResponse) error {
	iter := jsoniter.ConfigFastest.BorrowIterator(buf)
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "partial_success", "partialSuccess":
			dest.PartialSuccess = readExportTracePartialSuccess(iter)
		default:
			iter.Skip()
		}
		return true
	})
	return iter.Error
}

func readResourceSpans(iter *jsoniter.Iterator) *otlptrace.ResourceSpans {
	rs := &otlptrace.ResourceSpans{}
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "resource":
			json.ReadResource(iter, &rs.Resource)
		case "scopeSpans", "scope_spans":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				rs.ScopeSpans = append(rs.ScopeSpans, readScopeSpans(iter))
				return true
			})
		case "schemaUrl", "schema_url":
			rs.SchemaUrl = iter.ReadString()
		default:
			iter.Skip()
		}
		return true
	})
	return rs
}

func readScopeSpans(iter *jsoniter.Iterator) *otlptrace.ScopeSpans {
	ils := &otlptrace.ScopeSpans{}

	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "scope":
			json.ReadScope(iter, &ils.Scope)
		case "spans":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				ils.Spans = append(ils.Spans, readSpan(iter))
				return true
			})
		case "schemaUrl", "schema_url":
			ils.SchemaUrl = iter.ReadString()
		default:
			iter.Skip()
		}
		return true
	})
	return ils
}

func readSpan(iter *jsoniter.Iterator) *otlptrace.Span {
	sp := &otlptrace.Span{}

	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "traceId", "trace_id":
			if err := sp.TraceId.UnmarshalJSON([]byte(iter.ReadString())); err != nil {
				iter.ReportError("readSpan.traceId", fmt.Sprintf("parse trace_id:%v", err))
			}
		case "spanId", "span_id":
			if err := sp.SpanId.UnmarshalJSON([]byte(iter.ReadString())); err != nil {
				iter.ReportError("readSpan.spanId", fmt.Sprintf("parse span_id:%v", err))
			}
		case "traceState", "trace_state":
			sp.TraceState = iter.ReadString()
		case "parentSpanId", "parent_span_id":
			if err := sp.ParentSpanId.UnmarshalJSON([]byte(iter.ReadString())); err != nil {
				iter.ReportError("readSpan.parentSpanId", fmt.Sprintf("parse parent_span_id:%v", err))
			}
		case "name":
			sp.Name = iter.ReadString()
		case "kind":
			sp.Kind = otlptrace.Span_SpanKind(json.ReadEnumValue(iter, otlptrace.Span_SpanKind_value))
		case "startTimeUnixNano", "start_time_unix_nano":
			sp.StartTimeUnixNano = json.ReadUint64(iter)
		case "endTimeUnixNano", "end_time_unix_nano":
			sp.EndTimeUnixNano = json.ReadUint64(iter)
		case "attributes":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				sp.Attributes = append(sp.Attributes, json.ReadAttribute(iter))
				return true
			})
		case "droppedAttributesCount", "dropped_attributes_count":
			sp.DroppedAttributesCount = json.ReadUint32(iter)
		case "events":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				sp.Events = append(sp.Events, readSpanEvent(iter))
				return true
			})
		case "droppedEventsCount", "dropped_events_count":
			sp.DroppedEventsCount = json.ReadUint32(iter)
		case "links":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				sp.Links = append(sp.Links, readSpanLink(iter))
				return true
			})
		case "droppedLinksCount", "dropped_links_count":
			sp.DroppedLinksCount = json.ReadUint32(iter)
		case "status":
			iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
				switch f {
				case "message":
					sp.Status.Message = iter.ReadString()
				case "code":
					sp.Status.Code = otlptrace.Status_StatusCode(json.ReadEnumValue(iter, otlptrace.Status_StatusCode_value))
				default:
					iter.Skip()
				}
				return true
			})
		default:
			iter.Skip()
		}
		return true
	})
	return sp
}

func readSpanLink(iter *jsoniter.Iterator) *otlptrace.Span_Link {
	link := &otlptrace.Span_Link{}

	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "traceId", "trace_id":
			if err := link.TraceId.UnmarshalJSON([]byte(iter.ReadString())); err != nil {
				iter.ReportError("readSpanLink", fmt.Sprintf("parse trace_id:%v", err))
			}
		case "spanId", "span_id":
			if err := link.SpanId.UnmarshalJSON([]byte(iter.ReadString())); err != nil {
				iter.ReportError("readSpanLink", fmt.Sprintf("parse span_id:%v", err))
			}
		case "traceState", "trace_state":
			link.TraceState = iter.ReadString()
		case "attributes":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				link.Attributes = append(link.Attributes, json.ReadAttribute(iter))
				return true
			})
		case "droppedAttributesCount", "dropped_attributes_count":
			link.DroppedAttributesCount = json.ReadUint32(iter)
		default:
			iter.Skip()
		}
		return true
	})
	return link
}

func readSpanEvent(iter *jsoniter.Iterator) *otlptrace.Span_Event {
	event := &otlptrace.Span_Event{}

	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "timeUnixNano", "time_unix_nano":
			event.TimeUnixNano = json.ReadUint64(iter)
		case "name":
			event.Name = iter.ReadString()
		case "attributes":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				event.Attributes = append(event.Attributes, json.ReadAttribute(iter))
				return true
			})
		case "droppedAttributesCount", "dropped_attributes_count":
			event.DroppedAttributesCount = json.ReadUint32(iter)
		default:
			iter.Skip()
		}
		return true
	})
	return event
}

func readExportTracePartialSuccess(iter *jsoniter.Iterator) otlpcollectortrace.ExportTracePartialSuccess {
	lpr := otlpcollectortrace.ExportTracePartialSuccess{}
	iter.ReadObjectCB(func(iterator *jsoniter.Iterator, f string) bool {
		switch f {
		case "rejected_spans", "rejectedSpans":
			lpr.RejectedSpans = json.ReadInt64(iter)
		case "error_message", "errorMessage":
			lpr.ErrorMessage = iter.ReadString()
		default:
			iter.Skip()
		}
		return true
	})
	return lpr
}
