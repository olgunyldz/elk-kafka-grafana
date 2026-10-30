# Masks card and personal data in Logstash events (layer 3 of 3: the last gate before Elasticsearch).
# Same rules as the Java log-masking library (springboot/log-masking, SensitiveDataMasker), with checksums:
# card numbers (Luhn), IBAN (mod-97), Turkish national ID (TCKN), plus secrets and personal data by key,
# card expiry, email and phone.
#
# Used by both pipelines:
#   ruby {
#     path          => "/usr/share/logstash/pipeline/shared/mask_sensitive.rb"
#     script_params => { "fields" => ["message", "[payload]"] }   # strings, hashes and arrays are walked
#   }
# In a hash, the key counts too: {"cvv": "123"} or {"holderName": "Jane Doe"} is masked because of its key.
# Encrypted tokens from the producer (pan:v1:1111:...) are left alone: they're already safe, and their
# random payload could otherwise contain digits that look like a card number or phone.
# The tests at the bottom run every time a pipeline starts; if one fails, the pipeline doesn't start.

require 'set'

# Keys as a regex alternation; "_" also matches "-" and nothing; longest first (access_token before token)
ALTERNATION = ->(keys) { keys.map { |k| Regexp.escape(k).gsub('_', '[_-]?') }.sort_by { |k| -k.length }.join('|') }

SENSITIVE_KEYS = %w[
  password passwd pwd secret client_secret token access_token refresh_token id_token api_key apikey authorization
  cvv cvv2 cvc cvc2 cid security_code pin pin_block tckn tc_kimlik_no national_id
].freeze

# Names, birth dates and addresses have no pattern: the key decides (as the Java "personal" rule)
PERSONAL_KEYS = %w[
  first_name last_name full_name surname customer_name holder_name cardholder cardholder_name name_on_card
  birth_date date_of_birth dob address street street_address address_line1 address_line2 city postal_code zip
].freeze

# Hash keys, compared lower-case without '_', '-' or spaces
KEY_NAMES = (SENSITIVE_KEYS + PERSONAL_KEYS).map { |k| k.delete('_') }.to_set.freeze

TOKEN    = /\b(?:pan|iban|tckn|exp|email|phone|pii):v1:[A-Za-z0-9.-]*:[A-Za-z0-9_-]{38,}/
SECRETS  = /(["']?\b(?:#{ALTERNATION.call(SENSITIVE_KEYS)})\b["']?\s*[:=]\s*["']?)(?:(?:Bearer|Basic)\s+)?[^"'\s,;&}\[\]]*[^"'\s,;&}\[\].]/i
PERSONAL = /(["']?\b(?:#{ALTERNATION.call(PERSONAL_KEYS)})\b["']?\s*[:=]\s*["']?)(?![*\[{])[^"',;&{}\[\]\r\n]*[^"',;&{}\[\]\r\n\s]/i
EXPIRY   = /(["']?\b(?:exp(?:iry|iration)?(?:[_-]?date)?|card[_-]?exp(?:iry|iration)?(?:[_-]?date)?)\b["']?\s*[:=]\s*["']?)(?:\d{1,2}\s*\/\s*\d{2,4}|\d{4}-\d{2}(?:-\d{2})?)/i
IBAN     = /\b[A-Z]{2}\d{2}(?: ?[A-Z0-9]{4}){2,7}(?: ?[A-Z0-9]{1,4})?\b/
CARD     = /(?<![\d-])\d(?:[ -]?\d){12,18}(?![\d-])/
TCKN     = /(?<!\d)[1-9]\d{10}(?!\d)/
EMAIL    = /(?<![A-Za-z0-9._%+-])([A-Za-z0-9._%+-])[A-Za-z0-9._%+-]*@(?=[A-Za-z0-9-])/   # also when the domain is cut off
PHONE    = /(?<![\w+])\+\d(?:[ ().-]?\d){7,14}(?!\d)/
PHONE_TR = /(?<![\d*])0?5\d{2}(?:[ -]?\d){7}(?!\d)/

def register(params)
  @fields = params['fields'] || ['message']
end

def filter(event)
  @fields.each do |field|
    value = event.get(field)
    next if value.nil?
    masked = mask_value(value)
    event.set(field, masked) unless masked == value
  end
  [event]
end

# by_key: the value sits under a sensitive or personal key, so all of it is masked
def mask_value(value, by_key = false)
  case value
  when Hash    then value.each_with_object({}) { |(k, v), h| h[k] = mask_value(v, by_key || sensitive_key?(k)) }
  when Array   then value.map { |v| mask_value(v, by_key) }
  when String  then by_key ? mask_whole(value) : mask(value)
  when Numeric then by_key ? '***' : value
  else value
  end
end

def sensitive_key?(key)
  KEY_NAMES.include?(key.to_s.downcase.delete('_ -'))
end

# A value under a sensitive key: masked as a whole, unless it's already safe
def mask_whole(value)
  return value if value.empty? || value == '***' || value.start_with?('[REMOVED]') || value =~ /\A#{TOKEN}\z/
  '***'
end

# Masks the text between tokens; the tokens themselves stay as they are
def mask(text)
  out = +''
  pos = 0
  text.scan(TOKEN) do
    m = Regexp.last_match
    out << mask_plain(text[pos...m.begin(0)]) << m[0]
    pos = m.end(0)
  end
  out << mask_plain(text[pos..])
end

def mask_plain(text)
  return text if text.empty?
  text = text.gsub(SECRETS) { "#{$1}***" }
  text = text.gsub(PERSONAL) { "#{$1}***" }
  text = text.gsub(EXPIRY) { "#{$1}**/**" }
  text = text.gsub(IBAN) { |m| valid_iban?(m) ? keep_ends(m, 2, 4) : m }
  text = text.gsub(CARD) { |m| card?(m.delete('^0-9')) ? keep_ends(m, 0, 4) : m }
  text = text.gsub(TCKN) { |m| valid_tckn?(m) ? keep_ends(m, 0, 2) : m }
  text = text.gsub(EMAIL) { "#{$1}***@" }
  text = text.gsub(PHONE) { |m| keep_ends(m, 0, 2) }
  text.gsub(PHONE_TR) { |m| keep_ends(m, 0, 2) }
end

# Every letter or digit becomes '*' except the first keep_start and last keep_end; separators stay
def keep_ends(value, keep_start, keep_end)
  total = value.count('A-Za-z0-9')
  seen = 0
  value.chars.map do |c|
    if c =~ /[A-Za-z0-9]/
      visible = seen < keep_start || seen >= total - keep_end
      seen += 1
      visible ? c : '*'
    else
      c
    end
  end.join
end

# A card network prefix (2-6, or Troy 9792) and a valid Luhn check digit
def card?(digits)
  (('2'..'6').cover?(digits[0]) || digits.start_with?('9792')) && luhn?(digits)
end

def luhn?(digits)
  sum = digits.reverse.chars.each_with_index.sum do |c, i|
    d = c.to_i
    d *= 2 if i.odd?
    d > 9 ? d - 9 : d
  end
  (sum % 10).zero?
end

def valid_iban?(iban)
  s = iban.delete(' ')
  return false unless (15..34).cover?(s.length)
  numeric = (s[4..] + s[0, 4]).chars.map { |c| c =~ /\d/ ? c : (c.ord - 55).to_s }.join
  numeric.to_i % 97 == 1
end

def valid_tckn?(id)
  d = id.chars.map(&:to_i)
  odd = d[0] + d[2] + d[4] + d[6] + d[8]
  even = d[1] + d[3] + d[5] + d[7]
  d[9] == (odd * 7 - even) % 10 && d[10] == (odd + even + d[9]) % 10
end

# ---- Tests, run by Logstash when the pipeline starts ----

test 'card number: last 4 stay' do
  parameters { { 'fields' => ['message'] } }
  in_event { { 'message' => 'paid with 4111 1111 1111 1111 ok' } }
  expect('masked') { |events| events.first.get('message') == 'paid with **** **** **** 1111 ok' }
end

test 'secrets, IBAN, TCKN, email, phone' do
  parameters { { 'fields' => ['message'] } }
  in_event { { 'message' => '{"cvv":"123"} iban TR330006100519786457841326 tckn 10000000146 john.doe@example.com +90 532 123 45 67' } }
  expect('masked') do |events|
    events.first.get('message') ==
      '{"cvv":"***"} iban TR********************1326 tckn *********46 j***@example.com +** *** *** ** 67'
  end
end

test 'personal keys: the whole value, spaces included' do
  parameters { { 'fields' => ['message'] } }
  in_event { { 'message' => 'card={holderName=Jane Doe, expiry=03/28} address={street=Ataturk Cd. 1}' } }
  expect('masked') do |events|
    events.first.get('message') == 'card={holderName=***, expiry=**/**} address={street=***}'
  end
end

test 'nested payload: by value and by key' do
  parameters { { 'fields' => ['[payload]'] } }
  in_event do
    { 'payload' => { 'card' => '5555555555554444', 'cvv' => 737, 'holderName' => 'Jane Doe',
                     'address' => { 'city' => 'Istanbul' }, 'items' => ['mail jane@example.com'], 'amount' => 10 } }
  end
  expect('masked') do |events|
    p = events.first.get('payload')
    p['card'] == '************4444' && p['cvv'] == '***' && p['holderName'] == '***' &&
      p['address'] == { 'city' => '***' } && p['items'] == ['mail j***@example.com'] && p['amount'] == 10
  end
end

test 'encrypted tokens and removed values stay' do
  parameters { { 'fields' => ['message', '[payload]'] } }
  in_event do
    { 'message' => 'card pan:v1:1111:5555555555554444abcdefghijklmnopqrstuvwxyz0123 cvc=[REMOVED]',
      'payload' => { 'holderName' => 'pii:v1::bh5I162_hqluYukM96RhjuzlSv40lw4L9NumG2i_3oIjzhS_', 'cvv' => '[REMOVED]' } }
  end
  expect('unchanged') do |events|
    e = events.first
    e.get('message') == 'card pan:v1:1111:5555555555554444abcdefghijklmnopqrstuvwxyz0123 cvc=[REMOVED]' &&
      e.get('[payload][holderName]') == 'pii:v1::bh5I162_hqluYukM96RhjuzlSv40lw4L9NumG2i_3oIjzhS_' &&
      e.get('[payload][cvv]') == '[REMOVED]'
  end
end

test 'leaves normal log lines alone' do
  parameters { { 'fields' => ['message'] } }
  in_event { { 'message' => 'Key: order-42 Partition: 2 Offset: 41 Timestamp: 1793271793911 order 4111111111111112 name=cpu_usage_percent' } }
  expect('unchanged') do |events|
    events.first.get('message') == 'Key: order-42 Partition: 2 Offset: 41 Timestamp: 1793271793911 order 4111111111111112 name=cpu_usage_percent'
  end
end
