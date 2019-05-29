<?php
/**
 * Created by PhpStorm.
 * User: jamie
 * Date: 22/09/14
 * Time: 1:12 PM
 *
 * @package     Socket
 */

namespace Hazaar\Warlock\Protocol;

abstract class WebSockets {

    private   $magicGUID         = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private   $headers           = array();

    protected $allowed_protocols = array();

    function __construct($allowed_protocols = array()) {

        if(! is_array($allowed_protocols)) $allowed_protocols = explode(' ', $allowed_protocols);

        $this->allowed_protocols = $allowed_protocols;

    }

    public function parseHeaders($request) {

        $headers = array();

        $lines = explode("\n", $request);

        $lead = explode(' ', $lines[0], 3);

        if(! isset($lead[1])) return false;

        if(is_numeric($lead[1])) {

            $headers['code'] = intval($lead[1]);

            $headers['status'] = trim($lead[2]);

        } else {

            $headers[strtolower($lead[0])] = trim($lead[1]);

        }

        foreach($lines as $line) {

            $parts = explode(": ", $line);

            // Silently ignore bad headers
            if(count($parts) !== 2)
                continue;

            $headers[strtolower($parts[0])] = trim($parts[1]);

        }

        return $headers;

    }

    protected function createHandshake($path, $host, $origin, $key, $extra_headers = null) {

        $headers = array(
            'Host'                   => $host,
            'Connection'             => 'Upgrade',
            'Upgrade'                => 'WebSocket',
            'Sec-WebSocket-Key'      => $key,
            'Sec-WebSocket-Version'  => 13,
            'Sec-WebSocket-Protocol' => 'warlock'
        );

        if($origin)
            $header['Origin'] = $origin;

        if(is_array($extra_headers))
            $headers = array_merge($headers, $extra_headers);

        $requestHeaders = 'GET ' . $path . " HTTP/1.1\r\n";

        foreach($headers as $name => $value) 
            $requestHeaders .= $name . ': ' . $value . "\r\n";

        return $requestHeaders . "\r\n";

    }

    protected function acceptHandshake($headers, &$responseHeaders = array(), $key = NULL, &$results = array()) {

        if(! is_array($headers)) $headers = $this->parseHeaders($headers);

        $this->headers = $headers;

        if(array_key_exists('get', $headers)) {

            if(! array_key_exists('host', $headers) || ! ($results['host'] = $this->checkHost($headers['host']))) {

                return 400;

            }

            if(! ($results['url'] = $this->checkRequestURL($headers['get']))) {

                return 404;

            } elseif(array_key_exists('upgrade', $headers)) {
                /**
                 * New WebSockets Handshake
                 */

                if(! array_key_exists('connection', $headers) || strpos(strtolower($headers['connection']), 'upgrade') === FALSE) {

                    return 400;

                } elseif(strtolower($headers['upgrade']) !== 'websocket') {

                    return 400;

                } elseif(! array_key_exists('sec-websocket-key', $headers)) {

                    return 400;

                } elseif(! array_key_exists('sec-websocket-version', $headers) || intval($headers['sec-websocket-version']) != 13) {

                    $responseHeaders['Sec-WebSocket-Version'] = 13;

                    return 426;

                } elseif(array_key_exists('origin', $headers) && ! ($results['origin'] = $this->checkOrigin($headers['origin']))) {

                    return 403;

                } elseif(! array_key_exists('sec-websocket-protocol', $headers) || ! ($results['protocols'] = $this->checkProtocol($headers['sec-websocket-protocol']))) {

                    return 400;

                }

                $responseHeaders = array(
                    'Upgrade'                => 'websocket',
                    'Connection'             => 'Upgrade',
                    'Sec-WebSocket-Accept'   => base64_encode(sha1($this->headers['sec-websocket-key'] . $this->magicGUID, TRUE)),
                    'Sec-WebSocket-Protocol' => implode(', ', $results['protocols']),
                );

                return 101;

            }

        } elseif(array_key_exists('sec-websocket-accept', $headers)) {

            if(! array_key_exists('sec-websocket-accept', $headers) || base64_decode($headers['sec-websocket-accept']) != sha1($key . $this->magicGUID, TRUE)) {

                return FALSE;

            } elseif(! array_key_exists('sec-websocket-protocol', $headers) || count($this->checkProtocol($headers['sec-websocket-protocol'])) == 0) {

                return FALSE;

            }

            return TRUE;

        } else {

            return 405;

        }

        return FALSE;

    }

    protected function frame($payload, $type = NULL, $masked = TRUE) {

        if(! $type) $type = 'text';

        $frameHead = array();

        //Fix to correctly encode empty payloads
        if($masked && strlen($payload) == 0) $payload = ' ';

        $payloadLength = strlen($payload);

        switch($type) {

            case 'continuous':

                // first byte indicates FIN, Continuing-Frame (10000000):
                $frameHead[0] = 0x80;

                break;

            case 'text':

                // first byte indicates FIN, Text-Frame (10000001):
                $frameHead[0] = 0x81;

                break;

            case 'binary':

                // first byte indicates FIN, Binary-Frame (10000010):
                $frameHead[0] = 0x82;

                break;

            case 'close':

                // first byte indicates FIN, Close Frame(10001000):
                $frameHead[0] = 0x88;

                break;

            case 'ping':

                // first byte indicates FIN, Ping frame (10001001):
                $frameHead[0] = 0x89;

                break;

            case 'pong':

                // first byte indicates FIN, Pong frame (10001010):
                $frameHead[0] = 0x8A;

                break;
        }

        // set mask and payload length (using 1, 3 or 9 bytes)
        if($payloadLength > 65535) {

            $payloadLengthBin = str_split(sprintf('%064b', $payloadLength), 8);

            $frameHead[1] = ($masked === TRUE) ? 255 : 127;

            for($i = 0; $i < 8; $i++) {

                $frameHead[$i + 2] = bindec($payloadLengthBin[$i]);

            }

            // most significant bit MUST be 0 (close connection if frame too big)
            if($frameHead[2] > 127) {

                return FALSE;

            }

        } elseif($payloadLength > 125) {

            $payloadLengthBin = str_split(sprintf('%016b', $payloadLength), 8);

            $frameHead[1] = ($masked === TRUE) ? 254 : 126;

            $frameHead[2] = bindec($payloadLengthBin[0]);

            $frameHead[3] = bindec($payloadLengthBin[1]);

        } else {

            $frameHead[1] = ($masked === TRUE) ? $payloadLength + 128 : $payloadLength;

        }

        // convert frame-head to string:
        foreach(array_keys($frameHead) as $i)
            $frameHead[$i] = chr($frameHead[$i]);

        $mask = array();

        if($masked === TRUE) {

            // generate a random mask:
            for($i = 0; $i < 4; $i++)
                $mask[$i] = chr(rand(0, 255));

            $frameHead = array_merge($frameHead, $mask);

        }

        $frame = implode('', $frameHead);

        // append payload to frame:
        for($i = 0; $i < $payloadLength; $i++)
            $frame .= ($masked === TRUE) ? $payload[$i] ^ $mask[$i % 4] : $payload[$i];

        return $frame;

    }

    protected function getFrame(&$buffer, &$payload) {

        if(! $buffer)
            return FALSE;

        if(strlen($buffer) < 2)
            return -1;

        $payload = NULL;

        $headers = $this->getFrameHeaders($buffer);

        $offset = 2;

        if(ord($headers['hasmask']) > 0)
            $offset += 4;

        if($headers['length'] > 65535)
            $offset += 8;
        elseif($headers['length'] > 125)
            $offset += 2;

        if($headers['length'] > 0) {

            $payload = substr($buffer, $offset, $headers['length']);

            if($headers['length'] > strlen($payload))
                return -1;

        }

        //Truncate the buffer so that we leave whatever is left over
        $buffer = substr($buffer, $offset + $headers['length']);

        if($payload && ord($headers['hasmask']) > 0 && ! ($payload = $this->applyMask($headers, $payload)))
            return FALSE;

        if($headers['fin'])
            return $headers['opcode'];

        return false;

    }

    private function getFrameHeaders($frame) {

        $header = array(
            'fin'     => $frame[0] & chr(128),
            'rsv1'    => $frame[0] & chr(64),
            'rsv2'    => $frame[0] & chr(32),
            'rsv3'    => $frame[0] & chr(16),
            'opcode'  => ord($frame[0]) & 15,
            'hasmask' => $frame[1] & chr(128),
            'length'  => 0,
            'mask'    => ""
        );

        $header['length'] = (ord($frame[1]) >= 128) ? ord($frame[1]) - 128 : ord($frame[1]);

        if($header['length'] == 126) {

            if($header['hasmask']) {

                $header['mask'] = $frame[4] . $frame[5] . $frame[6] . $frame[7];

            }

            $header['length'] = ord($frame[2]) * 256 + ord($frame[3]);

        } elseif($header['length'] == 127) {

            if($header['hasmask']) {

                $header['mask'] = $frame[10] . $frame[11] . $frame[12] . $frame[13];

            }

            $header['length'] = ord($frame[2]) * 65536 * 65536 * 65536 * 256 + ord($frame[3]) * 65536 * 65536 * 65536 +
                                ord($frame[4]) * 65536 * 65536 * 256 + ord($frame[5]) * 65536 * 65536 + ord($frame[6]) * 65536 * 256 +
                                ord($frame[7]) * 65536 + ord($frame[8]) * 256 + ord($frame[9]);

        } elseif(ord($header['hasmask']) > 0) {

            $header['mask'] = $frame[2] . $frame[3] . $frame[4] . $frame[5];

        }

        return $header;

    }

    private function applyMask($headers, $payload) {

        $effectiveMask = "";

        if(ord($headers['hasmask']) > 0) {

            $mask = $headers['mask'];

        } else {

            return $payload;

        }

        if(strlen($mask) == 0) return FALSE;

        while(strlen($effectiveMask) < strlen($payload))
            $effectiveMask .= $mask;

        while(strlen($effectiveMask) > strlen($payload))
            $effectiveMask = substr($effectiveMask, 0, -1);

        return $effectiveMask ^ $payload;

    }

    protected function checkHost($host) {

        return ($host ? TRUE : FALSE);

    }

    protected function checkRequestURL($path) {

        return ($path ? TRUE : FALSE);

    }

    protected function checkOrigin($origin) {

        return ($origin ? TRUE : FALSE);

    }

    protected function checkProtocol($protocols) {

        $allowed = array();

        if(! is_array($protocols))
            $protocols = explode(',', $protocols);

        foreach($protocols as $proto) {

            $proto = strtolower(trim($proto));

            if(in_array($proto, $this->allowed_protocols))
                $allowed[] = $proto;
        }

        if(count($allowed) > 0)
            return $allowed;

        return FALSE;

    }

    protected function hexString($string) {

        $bytes = array();

        $len = strlen($string);

        for($i = 0; $i < $len; $i++)
            $bytes[] = str_pad(strtoupper(dechex(ord($string[$i]))), 2, '0', STR_PAD_LEFT);

        return $bytes;

    }

}