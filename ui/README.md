# sage-bigdata-etl-ui

## 介绍:
> sage-bigdata-etl前端ui模块，主要依赖bootstrap+angular,采用gulp构建;源文件目录app,构建目录dist

## 前端环境准备
 * 安装node.js,推荐安装4.x以上版本
 * 安装npm全局包gulp,`npm install gulp -g`;windows环境推荐cnpm替代npm,否则将导致各种错误
 * 安装npm全局包bower,`npm install bower -g`
 * 安装所有gulp依赖包`npm install`
 * 安装所有前端依赖包`bower install`

## 前端工作流程
 1. 执行任务gulp serve启动开发服务
 2. 编写或修改app下的代码,包括css,js和html
 3. 浏览器直接看到刷新后的效果

> 前端注意事项：为了更好的管理前端依赖，千万不要修改任何第三方的css文件和js文件！！！

## 后端工作流程
 1. 执行任务gulp default编译（在终端执行默认任务可直接输入gulp回车）
 2. 执行任务gulp deploy发布编译结果到sage-bigdata-etl中的resources/ui下,由于取的是相对路径,请将sage-bigdata-etl跟sage-bigdata-etl-ui放在一个工作空间下
