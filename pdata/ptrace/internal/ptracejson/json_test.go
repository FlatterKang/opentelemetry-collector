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

package ptracejson

import (
	"fmt"
	"github.com/gogo/protobuf/jsonpb"
	"go.opentelemetry.io/collector/pdata/internal/data"
	otlpcommon "go.opentelemetry.io/collector/pdata/internal/data/protogen/common/v1"
	v1 "go.opentelemetry.io/collector/pdata/internal/data/protogen/resource/v1"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	otlpcollectortrace "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/trace/v1"
	otlptrace "go.opentelemetry.io/collector/pdata/internal/data/protogen/trace/v1"
)

func TestReadTraceData(t *testing.T) {
	jsonStr := `{"extra":"", "resourceSpans": [{"extra":""}]}`
	value := &otlptrace.TracesData{}
	assert.NoError(t, UnmarshalTraceData([]byte(jsonStr), value))
	assert.Equal(t, &otlptrace.TracesData{ResourceSpans: []*otlptrace.ResourceSpans{{}}}, value)
}

func TestReadExportTraceServiceRequest(t *testing.T) {
	jsonStr := `{"extra":"", "resourceSpans": [{"extra":""}]}`
	value := &otlpcollectortrace.ExportTraceServiceRequest{}
	assert.NoError(t, UnmarshalExportTraceServiceRequest([]byte(jsonStr), value))
	assert.Equal(t, &otlpcollectortrace.ExportTraceServiceRequest{ResourceSpans: []*otlptrace.ResourceSpans{{}}}, value)
}

func TestReadExportTraceServiceResponse(t *testing.T) {
	jsonStr := `{"extra":"", "partialSuccess": {}}`
	value := &otlpcollectortrace.ExportTraceServiceResponse{}
	assert.NoError(t, UnmarshalExportTraceServiceResponse([]byte(jsonStr), value))
	assert.Equal(t, &otlpcollectortrace.ExportTraceServiceResponse{}, value)
}

func TestReadResourceSpans(t *testing.T) {
	jsonStr := `{"extra":"", "resource": {}, "schemaUrl": "schema", "scopeSpans": []}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	val := readResourceSpans(iter)
	assert.NoError(t, iter.Error)
	assert.Equal(t, &otlptrace.ResourceSpans{SchemaUrl: "schema"}, val)
}

func TestReadScopeSpans(t *testing.T) {
	jsonStr := `{"extra":"", "scope": {}, "logRecords": [], "schemaUrl": "schema"}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	val := readScopeSpans(iter)
	assert.NoError(t, iter.Error)
	assert.Equal(t, &otlptrace.ScopeSpans{SchemaUrl: "schema"}, val)
}

func TestReadSpan(t *testing.T) {
	jsonStr := `{"extra":""}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	val := readSpan(iter)
	assert.NoError(t, iter.Error)
	assert.Equal(t, &otlptrace.Span{}, val)
}

func TestReadSpanStatus(t *testing.T) {
	jsonStr := `{"status":{"extra":""}}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	val := readSpan(iter)
	assert.NoError(t, iter.Error)
	assert.Equal(t, &otlptrace.Span{}, val)
}

func TestReadSpanInvalidTraceIDField(t *testing.T) {
	jsonStr := `{"trace_id":"--"}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	readSpan(iter)
	if assert.Error(t, iter.Error) {
		assert.Contains(t, iter.Error.Error(), "parse trace_id")
	}
}

func TestReadSpanInvalidSpanIDField(t *testing.T) {
	jsonStr := `{"span_id":"--"}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	readSpan(iter)
	if assert.Error(t, iter.Error) {
		assert.Contains(t, iter.Error.Error(), "parse span_id")
	}
}

func TestReadSpanInvalidParentSpanIDField(t *testing.T) {
	jsonStr := `{"parent_span_id":"--"}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	readSpan(iter)
	if assert.Error(t, iter.Error) {
		assert.Contains(t, iter.Error.Error(), "parse parent_span_id")
	}
}

func TestReadSpanLink(t *testing.T) {
	jsonStr := `{"extra":""}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	val := readSpanLink(iter)
	assert.NoError(t, iter.Error)
	assert.Equal(t, &otlptrace.Span_Link{}, val)
}

func TestReadSpanLinkInvalidTraceIDField(t *testing.T) {
	jsonStr := `{"trace_id":"--"}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	readSpanLink(iter)
	if assert.Error(t, iter.Error) {
		assert.Contains(t, iter.Error.Error(), "parse trace_id")
	}
}

func TestReadSpanLinkInvalidSpanIDField(t *testing.T) {
	jsonStr := `{"span_id":"--"}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	readSpanLink(iter)
	if assert.Error(t, iter.Error) {
		assert.Contains(t, iter.Error.Error(), "parse span_id")
	}
}

func TestReadSpanEvent(t *testing.T) {
	jsonStr := `{"extra":""}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	val := readSpanEvent(iter)
	assert.NoError(t, iter.Error)
	assert.Equal(t, &otlptrace.Span_Event{}, val)
}

func TestReadExportTracePartialSuccess(t *testing.T) {
	jsonStr := `{"extra":"", "rejectedSpans":1, "errorMessage":"nothing"}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	value := readExportTracePartialSuccess(iter)
	assert.NoError(t, iter.Error)
	assert.Equal(t, otlpcollectortrace.ExportTracePartialSuccess{RejectedSpans: 1, ErrorMessage: "nothing"}, value)
}

func TestAnyValue(t *testing.T) {
	msg := otlpcommon.KeyValue{
		Key: "myKey",
		Value: otlpcommon.AnyValue{
			//Value: &otlpcommon.AnyValue_StringValue{StringValue: "my value"},
			Value: &otlpcommon.AnyValue_ArrayValue{
				ArrayValue: &otlpcommon.ArrayValue{
					Values: []otlpcommon.AnyValue{
						{Value: &otlpcommon.AnyValue_StringValue{StringValue: "my value"}},
					},
				},
			},
		},
	}
	var JSONMarshaler = &jsonpb.Marshaler{
		// https://github.com/open-telemetry/opentelemetry-specification/pull/2758
		EnumsAsInts: true,
		// https://github.com/open-telemetry/opentelemetry-specification/pull/2829
		OrigName: false,
	}

	str, err := JSONMarshaler.MarshalToString(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(str)
}

func TestMarshalExportTraceServiceRequest(t *testing.T) {
	type args struct {
		request *otlpcollectortrace.ExportTraceServiceRequest
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MarshalExportTraceServiceRequest(tt.args.request)
			if !tt.wantErr(t, err, fmt.Sprintf("MarshalExportTraceServiceRequest(%v)", tt.args.request)) {
				return
			}
			assert.Equalf(t, tt.want, got, "MarshalExportTraceServiceRequest(%v)", tt.args.request)
		})
	}
}

func TestMarshalExportTraceServiceResponse(t *testing.T) {
	type args struct {
		response *otlpcollectortrace.ExportTraceServiceResponse
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MarshalExportTraceServiceResponse(tt.args.response)
			if !tt.wantErr(t, err, fmt.Sprintf("MarshalExportTraceServiceResponse(%v)", tt.args.response)) {
				return
			}
			assert.Equalf(t, tt.want, got, "MarshalExportTraceServiceResponse(%v)", tt.args.response)
		})
	}
}

func TestMarshalTraceData(t *testing.T) {
	type args struct {
		traceData *otlptrace.TracesData
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MarshalTraceData(tt.args.traceData)
			if !tt.wantErr(t, err, fmt.Sprintf("MarshalTraceData(%v)", tt.args.traceData)) {
				return
			}
			assert.Equalf(t, tt.want, got, "MarshalTraceData(%v)", tt.args.traceData)
		})
	}
}

func Test_writeAnyValue(t *testing.T) {
	type args struct {
		st       *jsoniter.Stream
		anyValue otlpcommon.AnyValue
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, writeAnyValue(tt.args.st, tt.args.anyValue), fmt.Sprintf("writeAnyValue(%v, %v)", tt.args.st, tt.args.anyValue))
		})
	}
}

func Test_writeArrayValue(t *testing.T) {
	type args struct {
		st         *jsoniter.Stream
		arrayValue *otlpcommon.ArrayValue
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, writeArrayValue(tt.args.st, tt.args.arrayValue), fmt.Sprintf("writeArrayValue(%v, %v)", tt.args.st, tt.args.arrayValue))
		})
	}
}

func Test_writeKeyValue(t *testing.T) {
	type args struct {
		st *jsoniter.Stream
		kv otlpcommon.KeyValue
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, writeKeyValue(tt.args.st, tt.args.kv), fmt.Sprintf("writeKeyValue(%v, %v)", tt.args.st, tt.args.kv))
		})
	}
}

func Test_writeKeyValueList(t *testing.T) {
	type args struct {
		st          *jsoniter.Stream
		kvlistValue *otlpcommon.KeyValueList
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, writeKeyValueList(tt.args.st, tt.args.kvlistValue), fmt.Sprintf("writeKeyValueList(%v, %v)", tt.args.st, tt.args.kvlistValue))
		})
	}
}

func Test_writePartialSuccess(t *testing.T) {
	type args struct {
		st             *jsoniter.Stream
		partialSuccess otlpcollectortrace.ExportTracePartialSuccess
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writePartialSuccess(tt.args.st, tt.args.partialSuccess)
		})
	}
}

func Test_writeResource(t *testing.T) {
	type args struct {
		st       *jsoniter.Stream
		resource v1.Resource
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, writeResource(tt.args.st, tt.args.resource), fmt.Sprintf("writeResource(%v, %v)", tt.args.st, tt.args.resource))
		})
	}
}

func Test_writeResourceSpans(t *testing.T) {
	type args struct {
		st            *jsoniter.Stream
		resourceSpans *otlptrace.ResourceSpans
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, writeResourceSpans(tt.args.st, tt.args.resourceSpans), fmt.Sprintf("writeResourceSpans(%v, %v)", tt.args.st, tt.args.resourceSpans))
		})
	}
}

func Test_writeScopeSpans(t *testing.T) {
	type args struct {
		st         *jsoniter.Stream
		scopeSpans *otlptrace.ScopeSpans
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, writeScopeSpans(tt.args.st, tt.args.scopeSpans), fmt.Sprintf("writeScopeSpans(%v, %v)", tt.args.st, tt.args.scopeSpans))
		})
	}
}

func Test_writeSpan(t *testing.T) {
	type args struct {
		st   *jsoniter.Stream
		span *otlptrace.Span
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, writeSpan(tt.args.st, tt.args.span), fmt.Sprintf("writeSpan(%v, %v)", tt.args.st, tt.args.span))
		})
	}
}

func Test_writeSpanEvent(t *testing.T) {
	type args struct {
		st    *jsoniter.Stream
		event *otlptrace.Span_Event
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, writeSpanEvent(tt.args.st, tt.args.event), fmt.Sprintf("writeSpanEvent(%v, %v)", tt.args.st, tt.args.event))
		})
	}
}

func Test_writeSpanLink(t *testing.T) {
	var mockTraceID [16]byte
	copy(mockTraceID[:], "d10cee99624da16da09f57e9c139dfc6")
	var mockSpanID [8]byte
	copy(mockSpanID[:], "2107017ccdaaa09f")

	type args struct {
		st   *jsoniter.Stream
		link *otlptrace.Span_Link
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			"normal",
			args{
				st: jsoniter.ConfigCompatibleWithStandardLibrary.BorrowStream(nil),
				link: &otlptrace.Span_Link{
					TraceId:    data.TraceID(mockTraceID),
					SpanId:     data.SpanID(mockSpanID),
					TraceState: "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE",
					Attributes: []otlpcommon.KeyValue{{
						Key:   "testKey",
						Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "testValue"}},
					}},
					DroppedAttributesCount: 1,
				},
			},
			assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, writeSpanLink(tt.args.st, tt.args.link), fmt.Sprintf("writeSpanLink(%v, %v)", tt.args.st, tt.args.link))
			iter := jsoniter.ConfigFastest.BorrowIterator(tt.args.st.Buffer())
			defer jsoniter.ConfigFastest.ReturnIterator(iter)
			actual := readSpanLink(iter)
			if assert.NoError(t, iter.Error) {
				assert.Equal(t, tt.args.link, actual)
			}
		})
	}
}

func Test_writeStatus(t *testing.T) {
	type args struct {
		st     *jsoniter.Stream
		status otlptrace.Status
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"ok",
			args{
				st: jsoniter.ConfigCompatibleWithStandardLibrary.BorrowStream(nil),
				status: otlptrace.Status{
					Message: "",
					Code:    otlptrace.Status_STATUS_CODE_OK,
				},
			},
		},
		{
			"err",
			args{
				st: jsoniter.ConfigCompatibleWithStandardLibrary.BorrowStream(nil),
				status: otlptrace.Status{
					Message: "error msg",
					Code:    otlptrace.Status_STATUS_CODE_ERROR,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeStatus(tt.args.st, tt.args.status)
			actualStatus := otlptrace.Status{}
			if assert.NoError(t, jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(tt.args.st.Buffer(), &actualStatus)) {
				assert.Equal(t, tt.args.status, actualStatus)
			}
		})
	}
}
