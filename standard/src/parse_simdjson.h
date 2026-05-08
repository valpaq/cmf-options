// Reference NDJSON parser via simdjson `ondemand` — slow path used by
// ingest_opt. The hot path is fast_parse.h. We read every field even
// when we don't store it, so the OnDemand state machine stays on its
// in-order fast path.
#pragma once

#include "../../common/parse.h"

#include <simdjson.h>

#include <charconv>
#include <cmath>
#include <cstdint>
#include <string_view>

namespace ingest {

namespace ondemand = simdjson::ondemand;

// Field reads follow JSON order: required for OnDemand fast-path lookup.
inline MarketDataEvent parse_event(ondemand::document_reference doc) {
    MarketDataEvent evt;
    std::string_view sv;
    uint64_t v; int64_t iv;
    uint64_t ts_event_ns = 0;

    if (!doc["ts_recv"].get_string().get(sv))
        evt.ts_recv_ns = parse_timestamp_ns(sv);

    {
        ondemand::object hd;
        if (!doc["hd"].get_object().get(hd)) {
            if (!hd["ts_event"].get_string().get(sv))     ts_event_ns = parse_timestamp_ns(sv);
            if (!hd["rtype"].get_uint64().get(v))         (void)v;
            if (!hd["publisher_id"].get_uint64().get(v))  (void)v;
            if (!hd["instrument_id"].get_uint64().get(v)) evt.instrument_id = static_cast<uint32_t>(v);
        }
    }

    if (!doc["action"].get_string().get(sv)) evt.action = parse_action(sv);
    if (!doc["side"].get_string().get(sv))   evt.side   = parse_side(sv);

    {
        ondemand::value pv;
        if (!doc["price"].get(pv)) {
            ondemand::json_type t;
            if (!pv.type().get(t)) {
                if (t == ondemand::json_type::string) {
                    if (!pv.get_string().get(sv)) {
                        // UNDEF_PRICE sentinel (INT64_MAX) leaves has_price=false.
                        if (sv != UNDEF_PRICE_RAW) {
                            double d = parse_price_str(sv);
                            if (!std::isnan(d)) {
                                evt.price = d;
                                evt.has_price = true;
                            }
                        }
                    }
                } else if (t == ondemand::json_type::number) {
                    double d;
                    if (!pv.get_double().get(d)) {
                        evt.price = d;
                        evt.has_price = true;
                    }
                }
            }
        }
    }

    if (!doc["size"].get_uint64().get(v))       evt.size = v;
    if (!doc["channel_id"].get_uint64().get(v)) (void)v;

    if (!doc["order_id"].get_string().get(sv)) {
        uint64_t oid = 0;
        std::from_chars(sv.data(), sv.data() + sv.size(), oid);
        evt.order_id = oid;
    }

    if (!doc["flags"].get_uint64().get(v))       evt.flags = static_cast<uint32_t>(v);
    if (!doc["ts_in_delta"].get_int64().get(iv)) (void)iv;
    if (!doc["sequence"].get_uint64().get(v))    evt.sequence = v;

    evt.key_ts_ns = index_ts(evt.ts_recv_ns, ts_event_ns);
    return evt;
}

} // namespace ingest
