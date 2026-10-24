// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Standard base64 with padding, as Spark writes binary literals.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace spark::native::internal {

inline std::string Base64Encode(std::string_view bytes) {
  static constexpr char kAlphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string text;
  text.reserve((bytes.size() + 2) / 3 * 4);
  for (size_t index = 0; index < bytes.size(); index += 3) {
    size_t count = bytes.size() - index < 3 ? bytes.size() - index : 3;
    uint32_t bits = 0;
    for (size_t offset = 0; offset < 3; ++offset) {
      uint8_t byte = offset < count ? static_cast<uint8_t>(bytes[index + offset]) : 0;
      bits = (bits << 8) | byte;
    }
    for (size_t offset = 0; offset < 4; ++offset) {
      text += offset <= count ? kAlphabet[(bits >> (18 - 6 * offset)) & 0x3f] : '=';
    }
  }
  return text;
}

/// Decodes standard base64 with padding, or returns nothing for any other text.
inline std::optional<std::string> Base64Decode(std::string_view text) {
  auto value = [](char c) -> int {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
  };
  if (text.size() % 4 != 0) {
    return std::nullopt;
  }
  size_t groups = text.size() / 4;
  std::string bytes;
  bytes.reserve(groups * 3);
  for (size_t group = 0; group < groups; ++group) {
    std::string_view chars = text.substr(group * 4, 4);
    size_t padding = 0;
    while (padding < 4 && chars[3 - padding] == '=') {
      ++padding;
    }
    if (padding > 2 || (padding > 0 && group + 1 != groups)) {
      return std::nullopt;
    }
    uint32_t bits = 0;
    for (size_t index = 0; index < 4 - padding; ++index) {
      int digit = value(chars[index]);
      if (digit < 0) {
        return std::nullopt;
      }
      bits = (bits << 6) | static_cast<uint32_t>(digit);
    }
    bits <<= 6 * padding;
    for (size_t index = 0; index < 3 - padding; ++index) {
      bytes += static_cast<char>((bits >> (16 - 8 * index)) & 0xff);
    }
  }
  return bytes;
}

}  // namespace spark::native::internal
