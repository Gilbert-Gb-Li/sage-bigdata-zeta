// generated on 2016-06-09 using generator-webapp 2.0.0
import gulp from "gulp";
import gulpLoadPlugins from "gulp-load-plugins";
import browserSync from "browser-sync";
import del from "del";
import mainBowerFiles from "main-bower-files";
import {stream as wiredep} from "wiredep";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import url from "url";
import httpProxy from "http-proxy";


const proxyTarget = 'http://127.0.0.1:19090';
const $ = gulpLoadPlugins();
const reload = browserSync.reload;
const proxy = httpProxy.createProxyServer({});
//编译styles下的sass文件
gulp.task('styles', () => {
    return gulp.src('app/styles/*ss')
        .pipe($.plumber())
        .pipe($.sourcemaps.init())
        .pipe($.if('*.scss', $.sass.sync({
                outputStyle: 'expanded',
                precision: 10,
                includePaths: ['.']
            })).on('error', $.sass.logError)
        )
        .pipe($.if('*.less', $.less()))
        .pipe($.if('*_less.css',$.autoprefixer({browsers: ['> 1%', 'last 2 versions', 'Firefox ESR']})))
        .pipe($.sourcemaps.write('.'))
        .pipe(gulp.dest('.tmp/styles'));
});
//编译scripts下的js文件
gulp.task('scripts', () => {
    return gulp.src('app/scripts/**/*.js')
        .pipe($.plumber())
        .pipe($.sourcemaps.init())
        .pipe($.babel())
        .pipe($.ngAnnotate({single_quotes: true}))
        .pipe($.sourcemaps.write('.'))
        .pipe(gulp.dest('.tmp/scripts'))
        .pipe(reload({stream: true}));
});
//代码风格检测
function lint(files, options) {
    return () => {
        return gulp.src(files)
            .pipe(reload({stream: true, once: true}))
            .pipe($.eslint(options))
            .pipe($.eslint.format())
            .pipe($.if(!browserSync.active, $.eslint.failAfterError()));
    };
}
//test检测设置
const testLintOptions = {
    env: {
        mocha: true
    }
};
//检测scripts下所有js文件代码风格
gulp.task('lint', lint('app/scripts/**/*.js'));
//检测test下所有js文件代码风格
gulp.task('lint:test', lint('test/spec/**/*.js', testLintOptions));
//编译html文件，并处理js和css依赖（使用build和endbuild注释配置）
gulp.task('html', ['styles', 'scripts'], () => {
    return gulp.src(['app/**/*.html'])
        .pipe($.useref({searchPath: ['.tmp', 'app', '.']}))
        .pipe($.if('*.js', $.uglify()))
        .pipe($.if('*.css', $.cssnano({ zindex: false })))
        //暂时禁用修改文件名加版本号的方式
        /*.pipe($.if((file)=>{return /\.(?:js|css)$/.test(file.path)}, $.rev()))
         .pipe($.revReplace())*/
        .pipe($.if('*.html', $.htmlmin({
            collapseWhitespace: false,
            removeEmptyAttributes: true
        })))
        .pipe(gulp.dest('dist'));
});
//添加文件hash版本后缀
function addFileVersion(file) {
    let FILE_DECL = /(?:href=|src=)['|"]([^\s>"']+.min.[^\s>"']+?)['|"]/gi;
    let contents = file.contents.toString();
    let lines = contents.split('\n');
    let line, groups, filePath, data, hash;
    for (let i = 0, length = lines.length; i < length; i++) {
        line = lines[i];
        groups = FILE_DECL.exec(line);
        if (groups && groups.length > 1) {
            let uri = groups[1];
            if (uri.indexOf('//') === 0) {
                continue;
            }
            let normPath = path.normalize(uri);
            if (normPath.indexOf(path.sep) === 0) {
                filePath = path.join(file.base, normPath);
            } else {
                filePath = path.resolve(path.dirname(file.path), normPath);
            }
            let ext = path.extname(filePath);
            try {
                data = fs.readFileSync(filePath);
                hash = crypto.createHash('md5');
                hash.update(data.toString(), 'utf8');
                line = line.replace(ext, ext + '?v=' + hash.digest('hex').slice(0, 10));
            }
            catch (e) {
                console.log('addJsCssVersion error in file \'%s\'', filePath.red);
            }
        }
        lines[i] = line;
        FILE_DECL.lastIndex = 0;
    }
    file.contents = new Buffer(lines.join('\n'));
}
//处理版本号
gulp.task('version', ['html'], () => {
    return gulp.src('dist/**/*.html')
        .pipe($.tap(addFileVersion))
        .pipe(gulp.dest('dist'));
});
//压缩images下的图片文件
gulp.task('images', () => {
    return gulp.src('app/images/**/*')
        .pipe($.cache($.imagemin({
            progressive: true,
            interlaced: true,
            // don't remove IDs from SVGs, they are often used
            // as hooks for embedding and styling
            svgoPlugins: [{cleanupIDs: false}]
        })))
        .pipe(gulp.dest('dist/images'));
});
//处理字体文件
gulp.task('fonts', () => {

    let files = mainBowerFiles('**/*.{eot,svg,ttf,woff,woff2}', function (err) {
        if (err)
            console.error('编译字体异常  \'%s\'',err);
    });
    return gulp.src(files
        .concat('app/fonts/**/*'))
        .pipe(gulp.dest('.tmp/fonts'))
        .pipe(gulp.dest('dist/fonts'));
});
//处理其他文件
gulp.task('extras', () => {
    //拷贝 ztree 图片文件
    let ztreeStyle = 'metroStyle';
    gulp.src(['bower_components/zTree/css/' + ztreeStyle + '/**', '!**/*.css'])
        .pipe(gulp.dest('dist/styles'));
    return gulp.src([
        'app/**/*.*',
        '!**/*.html',
        '!**/*.js',
        '!**/*.css',
        '!**/*.less',
        '!**/*.scss',
        '!**/images/**',
        '!**/*.json'
    ], {
        dot: true
    }).pipe(gulp.dest('dist'));

});
//清除编译的临时文件
gulp.task('clean', del.bind(null, ['.tmp', 'dist']));
//代理api接口
function proxyAPI(req, res) {
    let url_parts = url.parse(req.url);
    let pathname = url_parts.pathname;
    if (pathname.endsWith('/') || pathname.indexOf('.') != -1) {
        return false;
    }
    proxy.web(req, res, {target: proxyTarget});
}
proxy.on('error', function (err, req) {
    console.log("proxy error!!! url:%s\n%s".red, req.url, err);
});
//启动服务,并监听文件变化，时时刷新
gulp.task('serve', ['styles', 'scripts', 'fonts'], () => {
    browserSync({
        notify: false,
        port: 9000,
        server: {
            baseDir: ['.tmp', 'app'],
            routes: {
                '/bower_components': 'bower_components'
            },
            middleware: function (req, res, next) {
                if (proxyAPI(req, res) == false) {
                    next();
                }
            }
        }
    });

    gulp.watch([
        'app/*.html',
        'app/**/*.html',
        'app/images/**/*',
        '.tmp/fonts/**/*'
    ]).on('change', reload);

    gulp.watch('app/styles/**/*ss', ['styles']);
    gulp.watch('app/scripts/**/*.js', ['scripts']);
    gulp.watch('app/fonts/**/*', ['fonts']);
    gulp.watch('bower.json', [/*'wiredep', */'fonts']);
});
//在dist下启动服务
gulp.task('serve:dist', () => {
    browserSync({
        notify: false,
        port: 9000,
        server: {
            baseDir: ['dist']
        },
        middleware: function (req, res, next) {
            if (proxyAPI(req, res) == false) {
                next();
            }
        }
    });
});
//在test下启动服务
gulp.task('serve:test', ['scripts'], () => {
    browserSync({
        notify: false,
        port: 9000,
        ui: false,
        server: {
            baseDir: 'test',
            routes: {
                '/scripts': '.tmp/scripts',
                '/bower_components': 'bower_components'
            }
        }
    });

    gulp.watch('app/scripts/**/*.js', ['scripts']);
    gulp.watch('test/spec/**/*.js').on('change', reload);
    gulp.watch('test/spec/**/*.js', ['lint:test']);
});

//自动在html中注入前端依赖组件，未测试，慎用！！！
gulp.task('wiredep', () => {
    gulp.src('app/styles/*.scss')
        .pipe(wiredep({
            ignorePath: /^(\.\.\/)+/
        }))
        .pipe(gulp.dest('app/styles'));

    gulp.src('app/**/*.html')
        .pipe(wiredep({
            exclude: ['bootstrap-sass'],
            ignorePath: /^(\.\.\/)*\.\./
        }))
        .pipe(gulp.dest('app'));
});
//构建所有,注释lint取消代码检查
gulp.task('build', ['version', 'images', 'fonts', 'extras','styles'], () => {
    return gulp.src('dist/**/*').pipe($.size({title: 'build', gzip: true}));
});
//清空并构建所有
gulp.task('default', ['clean'], () => {
    gulp.start('build');
});
//发布构建
gulp.task('deploy',['build'], () => {
    let deployDir = '../master/src/main/resources/ui';
    del([deployDir + '/**', '!' + deployDir], {force: true}).then(function () {

        let dest=gulp.dest(deployDir);
        console.log('Deploy build to dir: \'%s\'', deployDir);

        gulp.src('dist/**/*').pipe(dest);


        console.log('Deploy build to dir: \'%s\' done!', deployDir);
    });
});
