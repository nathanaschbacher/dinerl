-module(dynamodb).
-author('Valentino Volonghi <valentino@adroll.com>').

-include("dinerl_types.hrl").

-export([endpoint/1, signature_header/7, call/7, call/8, signature_header_4/7]).


-spec endpoint(zone()) -> endpoint().
endpoint("us-east-1" ++ _R) -> "dynamodb.us-east-1.amazonaws.com";
endpoint("us-west-1" ++ _R) -> "dynamodb.us-west-1.amazonaws.com";
endpoint("us-west-2" ++ _R) -> "dynamodb.us-west-2.amazonaws.com";
endpoint("ap-northeast-1" ++ _R) -> "dynamodb.ap-northeast-1.amazonaws.com";
endpoint("ap-southeast-1" ++ _R) -> "dynamodb.ap-southeast-1.amazonaws.com";
endpoint("eu-west-1" ++ _R) -> "dynamodb.eu-west-1.amazonaws.com".

-spec region(endpoint()) -> region().
region("dynamodb.us-east-1.amazonaws.com") -> "us-east-1";
region("dynamodb.us-west-1.amazonaws.com") -> "us-west-1";
region("dynamodb.us-west-2.amazonaws.com") -> "us-west-2";
region("dynamodb.ap-northeast-1.amazonaws.com") -> "ap-northeast-1";
region("dynamodb.ap-southeast-1.amazonaws.com") -> "ap-southeast-1";
region("dynamodb.eu-west-1.amazonaws.com") -> "eu-west-1".

hex_from_bin(Md5_bin) ->
    Md5_list = binary_to_list(Md5_bin),
    list_to_binary(lists:flatten(list_to_hex(Md5_list))).

list_to_hex(L) ->
    lists:map(fun(X) -> int_to_hex(X) end, L).

int_to_hex(N) when N < 256 ->
    [hex(N div 16), hex(N rem 16)].

hex(N) when N < 10 ->
    $0+N;
hex(N) when N >= 10, N < 16 ->
    $a + (N-10).

signature_header(AccessKeyId, SecretAccessKey, Target, Token, Date, EndPoint, Body) ->
    SignString = ["POST", $\n,
                  "/", $\n,
                  $\n,
                  "host:", EndPoint, $\n,
                  "x-amz-date:", Date, $\n,
                  "x-amz-security-token:", Token, $\n,
                  "x-amz-target:", Target, $\n,
                  $\n,
                  Body],
    StringToSign = crypto:sha(SignString),
    Signature = base64:encode_to_string(crypto:sha_mac(SecretAccessKey, StringToSign)),
    {ok,
     {"x-amzn-authorization",
      ["AWS3 AWSAccessKeyId=",
       AccessKeyId,
       ",Algorithm=HmacSHA1,SignedHeaders=host;x-amz-date;x-amz-security-token;x-amz-target,Signature=",
       Signature]}}.

signature_header_4(AccessKeyId, SecretAccessKey, Target, _Token, Date, EndPoint, Body) ->
    DDate = lists:sublist(Date, 6),
    Region = region(EndPoint),

    % % Changes once a day
    % Start = crypto:sha256_mac(["AWS4", SecretAccessKey], DDate),
    % SigningKey = lists:foldl(fun(X, Acc) -> crypto:sha256_mac(Acc, X) end,
    %                          Start,
    %                          [Region, "dynamodb", "aws4_request"]),
    SigningKey = "absbakadkdaskads",

    % Changes all the time
    SignString = ["POST", $\n,
                  "/", $\n,
                  $\n,
                  "host:", EndPoint, $\n,
                  "x-amz-date:", Date, $\n,
                  "x-amz-target:", Target, $\n,
                  "host;x-amz-date;x-amz-target", $\n,
                  hex_from_bin(crypto:sha256(Body))],
    CanonicalRequest = hex_from_bin(crypto:sha256(SignString)),

    StringToSign = ["AWS4-HMAC-SHA256", $\n,
                    Date, $\n,
                    DDate, "/", Region, "/dynamodb/aws4_request", $\n,
                       hex_from_bin(crypto:sha256(CanonicalRequest))],


    Signature = hex_from_bin(crypto:sha256_mac(SigningKey, StringToSign)),
    {ok,
     {"Authorization",
      ["AWS4-HMAC-SHA Credential=",
       AccessKeyId, "/", DDate, "/", Region, "/dynamodb/aws4_request",
       ",SignedHeaders=host;x-amz-date;x-amz-target,Signature=",
       Signature]}}.




-spec call(access_key_id(), secret_access_key(),
           zone(), string(), token(), rfcdate(),
           any()) -> result().
call(AccessKeyId, SecretAccessKey, Zone, Target, Token, RFCDate, Body) ->
    call(AccessKeyId, SecretAccessKey, Zone, Target, Token, RFCDate, Body, 1000).

-spec call(access_key_id(), secret_access_key(),
           zone(), string(), token(), rfcdate(),
           any(), integer()) -> result().
call(AccessKeyId, SecretAccessKey, Zone, Target, Token, RFCDate, Body, undefined) ->
    call(AccessKeyId, SecretAccessKey, Zone, Target, Token, RFCDate, Body, 1000);
call(AccessKeyId, SecretAccessKey, Zone, Target, Token, RFCDate, Body, Timeout) ->
    EndPoint = endpoint(Zone),
    {ok, SHeader} = signature_header(AccessKeyId, SecretAccessKey, Target,
                                     Token, RFCDate, EndPoint, Body),
    submit("http://" ++ EndPoint ++ "/",
           [{"content-type", "application/x-amz-json-1.0"},
            {"x-amz-date", RFCDate},
            {"x-amz-security-token", Token},
            {"x-amz-target", Target}, SHeader], Body, Timeout).


-spec submit(endpoint(), headers(), any(), integer()) -> result().
submit(Endpoint, Headers, Body, Timeout) ->
    %io:format("Request:~nHeaders:~p~nBody:~n~p~n~n", [Headers, iolist_to_binary(Body)]),
    case lhttpc:request(Endpoint, "POST", Headers, Body, Timeout, [{max_connections, 5000}]) of
        {ok, {{200, _}, _Headers, Response}} ->
            %io:format("Response: ~p~n", [Response]),
            {ok, Response};
        {ok, {{400, Code}, _Headers, ErrorString}} ->
            {error, Code, ErrorString};
        {ok, {{413, Code}, _Headers, ErrorString}} ->
            {error, Code, ErrorString};
        {ok, {{500, Code}, _Headers, ErrorString}} ->
            {error, Code, ErrorString};
        {error, Reason} ->
            {error, unknown, Reason};
        Other ->
            {error, response, Other}
    end.
