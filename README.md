# MoviePilot-Plugins

MoviePilot三方插件：https://github.com/miniers/MoviePilot-Plugins

## 安装说明

MoviePilot环境变量添加本项目地址，具体参见 https://github.com/jxxghp/MoviePilot

## 插件说明


### [Plex演职人员刮削](https://github.com/miniers/MoviePilot-Plugins/blob/main/plugins/plexpersonmeta/README.md)

- 实现刮削演职人员中文名称及角色
- Plex 的 API 实现较为复杂，我在尝试为 `actor.tag.tagKey` 赋值时遇到了问题，如果您对此有所了解，请不吝赐教，可以通过新增一个 issue 与我联系，特此感谢
- **警告**：由于 `tagKey` 的问题，当执行刮削后，可能会出现丢失在线元数据，无法在Plex中点击人物查看详情等问题

#### 感谢

- 本插件基于 [InfinityPacer/MoviePilot-Plugins](https://github.com/InfinityPacer/MoviePilot-Plugins) 修改，修复了新版缓存调用问题。
- 如有未能提及的作者，请告知我以便进行补充。