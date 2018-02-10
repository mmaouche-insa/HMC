<?php
$endpoint = 'http://liris-vm-27.insa-lyon.fr:8888/api';

$url = $endpoint . $_SERVER['PATH_INFO'] . '?' . http_build_query($_GET);

$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, $url);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
curl_setopt($ch, CURLOPT_HEADER, 1);
$response = curl_exec($ch);

$headerSize = curl_getinfo($ch, CURLINFO_HEADER_SIZE);
$headers = explode("\n", substr($response, 0, $headerSize));
$body = substr($response, $headerSize);

curl_close($ch);

foreach ($headers as $header) {
    header($header);
}

echo $body;