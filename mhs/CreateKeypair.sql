# 2048-bit RSA private key 생성
-- openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt -- no password --AFWAP03
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out rsa_key.p8 -- password 입력 방식 --CDCAP01


# Private key로부터 public key 추출 
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub


# 2. 공개 키 추출
# 개인 키에서 공개 키를 추출합니다.
