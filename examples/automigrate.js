var DataSource = require('loopback-datasource-juggler').DataSource;
var db = new DataSource(require('../index'), {
  contactPoints: [ '127.0.0.1' ],
  keyspace: 'demo'
  
});
var model = {
  'title': {'type': String},
  'description': {'type': String}
};
var Movie = db.define('Movie', model);
var movies = [
  {title: 'Benher', description: 'Histry'},
  {title: 'Titanic', description: 'Romance'},
  {title: 'Rambo', description: 'Action'},
  
];

 db.automigrate(undefined,function() {
     console.log('Created Table !!');
     Movie.create(movies, function (err, h) {
         if(err){
             console.log(err);
         } else {
             console.log("Inserted :", h);
             //Movie.find({where: {title: 'Rambo'}}, function (err, arr) {
             //  console.log("Find' : \n", arr);
             // });
         }
     });
 });
