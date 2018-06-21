const hashnethelper = require('../ice/hashnethelper.js');

hashnethelper.sendMessageDirect('xx', 'yy').then((res) => {
    console.log(res);
});

