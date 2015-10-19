var express            = require('express');
var app                = express();
var server             = require('http').Server(app);

app.use(express.static(__dirname + '/public'));

var port = 3000;

server.listen(port, function() {
    console.log('DistributedCV GUI on port', port);
});
