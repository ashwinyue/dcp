#!/bin/bash

# Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file. The original repo for
# this file is https://github.com/onexstack/miniblog. The professional
# version of this repository is https://github.com/onexstack/onex.


# 定义Header
HEADER='{"alg":"HS256","typ":"JWT"}'

# 定义Payload
PAYLOAD='{"exp":1739078005,"iat":1735478005,"nbf":1735478005,"x-user-id":"user-w6irkg"}'

# 定义Secret（用于签名）
SECRET="Rtg8BPKNEf2mB4mgvKONGPZZQSaJWNLijxR42qRgq0iBb5"

# 1. Base64编码Header
HEADER_BASE64=$(echo -n "${HEADER}" | openssl base64 | tr -d '=' | tr '/+' '_-' | tr -d '\n')

# 2. Base64编码Payload
PAYLOAD_BASE64=$(echo -n "${PAYLOAD}" | openssl base64 | tr -d '=' | tr '/+' '_-' | tr -d '\n')

# 3. 拼接Header和Payload为签名数据
SIGNING_INPUT="${HEADER_BASE64}.${PAYLOAD_BASE64}"

# 4. 使用HMAC SHA256算法生成签名
SIGNATURE=$(echo -n "${SIGNING_INPUT}" | openssl dgst -sha256 -hmac "${SECRET}" -binary | openssl base64 | tr -d '=' | tr '/+' '_-' | tr -d '\n')

# 5. 拼接最终的JWT Token
JWT="${SIGNING_INPUT}.${SIGNATURE}"

# 输出JWT Token
echo "Generated JWT Token:"
echo "${JWT}"
