process.on('message', function (data) {
  process.send({
    Message: 'Received data',
    Response: data.length,
  });
});
