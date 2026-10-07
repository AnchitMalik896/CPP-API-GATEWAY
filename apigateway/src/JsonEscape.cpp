// src/JsonEscape.cpp
#include "JsonEscape.hpp"

namespace apigateway {

namespace {

constexpr char kHexDigits[] = "0123456789abcdef";

void appendUnicodeEscape(std::string& out, unsigned int codeUnit) {
    out.append("\\u");
    out.push_back(kHexDigits[(codeUnit >> 12) & 0xFU]);
    out.push_back(kHexDigits[(codeUnit >> 8) & 0xFU]);
    out.push_back(kHexDigits[(codeUnit >> 4) & 0xFU]);
    out.push_back(kHexDigits[codeUnit & 0xFU]);
}

bool isContinuationByte(std::string_view s, size_t index) noexcept {
    return index < s.size() && (static_cast<unsigned char>(s[index]) & 0xC0U) == 0x80U;
}

// Length of the well-formed UTF-8 sequence starting at s[i], or 0 if the
// bytes there are not well-formed (overlong, surrogate, > U+10FFFF, truncated).
size_t utf8SequenceLength(std::string_view s, size_t i) noexcept {
    const auto lead = static_cast<unsigned char>(s[i]);

    if (lead >= 0xC2 && lead <= 0xDF) {
        return isContinuationByte(s, i + 1) ? 2 : 0;
    }

    if (lead >= 0xE0 && lead <= 0xEF) {
        if (!isContinuationByte(s, i + 1) || !isContinuationByte(s, i + 2)) {
            return 0;
        }
        const auto second = static_cast<unsigned char>(s[i + 1]);
        if (lead == 0xE0 && second < 0xA0) {
            return 0;
        }
        if (lead == 0xED && second > 0x9F) {
            return 0;
        }
        return 3;
    }

    if (lead >= 0xF0 && lead <= 0xF4) {
        if (!isContinuationByte(s, i + 1) || !isContinuationByte(s, i + 2) ||
            !isContinuationByte(s, i + 3)) {
            return 0;
        }
        const auto second = static_cast<unsigned char>(s[i + 1]);
        if (lead == 0xF0 && second < 0x90) {
            return 0;
        }
        if (lead == 0xF4 && second > 0x8F) {
            return 0;
        }
        return 4;
    }

    return 0;
}

}

void appendJsonEscaped(std::string& out, std::string_view in) {
    size_t i = 0;
    while (i < in.size()) {
        const auto byte = static_cast<unsigned char>(in[i]);

        if (byte == '"') {
            out.append("\\\"");
        } else if (byte == '\\') {
            out.append("\\\\");
        } else if (byte == '\n') {
            out.append("\\n");
        } else if (byte == '\r') {
            out.append("\\r");
        } else if (byte == '\t') {
            out.append("\\t");
        } else if (byte == '\b') {
            out.append("\\b");
        } else if (byte == '\f') {
            out.append("\\f");
        } else if (byte < 0x20 || byte == 0x7F) {
            appendUnicodeEscape(out, byte);
        } else if (byte < 0x80) {
            out.push_back(static_cast<char>(byte));
        } else {
            const size_t length = utf8SequenceLength(in, i);
            if (length == 0) {
                out.append("\\ufffd");
            } else {
                out.append(in.substr(i, length));
                i += length;
                continue;
            }
        }
        ++i;
    }
}

}
