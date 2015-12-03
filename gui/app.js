var path               = require('path');
var express            = require('express');
var app                = express();
var server             = require('http').Server(app);
var io                 = require('socket.io')(server);
var PythonShell        = require('python-shell');



app.use(express.static(__dirname + '/public'));

var port = 3000;

server.listen(port, function() {
    console.log('DistributedCV GUI on port', port);
});

io.on('connection', function(socket) {

    var clusterDir = path.join(__dirname, '..', 'cluster');
    var clusterScript = 'cluster.py';
    
    process.chdir(clusterDir);

    socket.on('shellInput', function(args) {
        
        var pyshell = new PythonShell(clusterScript, {
            mode: 'text',
            pythonOptions: ['-u'],
            scriptPath: clusterDir,
            args: args
        });

        pyshell.on('message', function (message) {
          console.log(message);
          socket.broadcast.emit('shellOutput', message);
        });

        pyshell.end(function (err) {
          if (err) throw err;
          console.log('finished');
        });

    });

});