module.exports = function (grunt) {
    //配置参数  
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),

        babel: {
            options: {
                sourceMap: false,
                presets: ['babel-preset-es2015']

            },
            dist: {
                files: [{
                    expand: true,
                    cwd: 'src/', //js目录下
                    src: ['**/vuedraggable.js'], //所有js文件
                    dest: 'dist/'  //输出到此目录下
                }]
            }
        },
    });
    //注册任务  
    grunt.registerTask('default', ['babel']);
} 