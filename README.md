
<h1>shusl增加的优化</h1>
1. 增加了绑定指定IP的能力 <br/>
2. 增加了基于int类型字段hash取模的支持（比如分100个库）<br/>
3. 增加基于字符串类型，进行CRC32之后，进行hash取模分库的支持<br/>

<h1>codertianwei增加的优化</h1>
1. 增加ServerIdMask和FileMap的复合路由 <br/>
2. 优化配置，相同库的表只需配置一次 <br/>

<br/>

![](https://raw.githubusercontent.com/alibaba/cobar/master/doc/Cobar_logo.png)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![Project Status](https://img.shields.io/badge/status-release-green.svg)

## What is Cobar?

Cobar is a proxy for sharding databases and tables,compatible with MySQL protocal and MySQL SQL grama,underlying storage only support MySQL for support foreground business more simple,stable,efficient and safety。

- __Sharding__
You can add new MySQL instance as your business grows.

- __High availability__
Both Cobar server and underlying MySQL is clustered,business will not suffer with single node fail.

- __Compatible with MySQL protocol__
Use Cobar as MySQL. You can replace MySQL with Cobar to power your application.

## Roadmap

Read the [Roadmap](https://github.com/alibaba/cobar/wiki/RoadMap).

## Architecture

![](https://raw.githubusercontent.com/alibaba/cobar/master/doc/Cobar_architecture.png)

## Quick start

Read the [Quick Start](https://github.com/alibaba/cobar/wiki/Quick-Start).

## Documentation

+ [User Guide](https://github.com/alibaba/cobar/wiki/User--Guide)
+ [FAQ](https://github.com/alibaba/cobar/wiki/FAQ)


## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](https://github.com/alibaba/cobar/blob/master/CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## Mailing list
alibaba_cobar@googlegroups.com


## License
Cobar is under the Apache 2.0 license. See the [LICENSE](https://github.com/alibaba/cobar/blob/master/LICENSE) file for details.
