# ReactiveThespian

A reactive streaming tool for building an ETL/Ingestion backbone. This tool offers distributed and reactive streams built for Python. An attempt is made to implement at least most of the reactive streams protocol. 

# When to Use ReactiveThespian

Reactive Thespian should be used when in need of streaming. ETL/Ingestion where a graphical processor is important benefit immensely from this model. The project will try to be non-blocking but due to some constant issues with streams discovered in CompAktor, this may be more difficult than it seems.

# Thespian

Thespian is actively developed by Kevin Quick and maintained by GoDaddy. At some point, I hope to take the basic actors I have working in CompAktor and fix the streams module in that tool. However, Thespian, which does not use asyncio, is the best alternative at the moment. Asyncio needs to improve to exit a loop quickly and return control from await only when a loop exits and not a function completes (perhaps a race condition). Until then, checkout Thespian (https://github.com/godaddy/Thespian), an actively developed and strong project.

# Completed Tasks

This project is new. Please help bring a solid reactive, non-mimicked reactive model to Python. I've been looking to port Akka for a while. The basic actor and remoting were enacted in Thespian

# Goal

The following goals need to be met:

- Routers
- Streams
- Distributed Actors and registries
- A cluster system

# Implementation

Implementation can be found at https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.1/README.md

# License

Copyright 2017- Andrew Evans and SimplrInsites, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


