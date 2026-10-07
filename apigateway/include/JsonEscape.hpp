#pragma once

#include <string>
#include <string_view>

namespace apigateway {

// Appends `in` to `out` as the contents of a JSON string literal; the
// surrounding quotes are the caller's. Escapes '"', '\\' and every control
// character (U+0000-U+001F, U+007F). Bytes that are not valid UTF-8 become
// U+FFFD, so the output is always a valid JSON string body.
void appendJsonEscaped(std::string& out, std::string_view in);

}
