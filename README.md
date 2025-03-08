# Test with InputStream and Quarkus

This code is to demonstrate issues with Quarkus and in particular with InpuStream.

Note that in context of Container, having File or huge memory is not a valid option AFAIK.

InputStream should be a correct way, while consuming only limited memory and no file.

## Goals

Comparing Netty native and Quarkus way for Client side, while Netty is used behind the scene with Quarkus.

- All clients target the very same Quarkus Server implementation
- No real file or in memory InputStream is created: FakeInputStream simulate an InputStream

## Experimentation

Tests were done using 1 GB fake InputStream 10 times

- Reading (GET) InputStream using Quarkus Client: 459 MB/s (2.2s) with 42 MB memory consumption
- Writing (POST) InputStream:
    - Using Quarkus Client: 25.4 MB/s (50.8s) and 39 MB of Memory consumption
    - Using Quarkus Client Injected: 25.0 MB/s (40.9s) and 44 MB of Memory consumption
    - Using Netty Client:  387 MB/s (2.6s) and 44 MB of Memory consumption

Of course, with lower size, the difference is almost not visible.
With 1MB (Change to be done in StaticValues class) 20 times

- Read using Quarkus Client: 250 MB/s (4ms) and 20 MB Memory consumption
- Writing (POST) InputStream:
    - Using Quarkus Client: 19.7 MB/s (47ms) and 18 MB Memory consumption
    - Using Quarkus Client Injected: 23.7 MB/s (42ms) and 57 MB Memory consumption
    - Using Netty Client: 264.8 MB/s (4ms) and 24 MB Memory consumption

## Analysis

- No issue on READ (GET)
- But huge issue in WRITE (POST)
    - There is a huge difference in time between Quarkus and Netty on client side, which should not be
      - Difference is close to a factor of 10.
    - The memory is not affected
