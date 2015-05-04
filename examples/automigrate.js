var DataSource = require('loopback-datasource-juggler').DataSource;
var db = new DataSource(require('../index'), {
    contactPoints: ["127.0.0.1"],
    keyspace: "demo",
    partitionKey:"title",
    clustaringKeys:["description"]

});
var model = {
    'title': {'type': String},
    'description': {'type': String},
    'Language': {'type': String}
};
var Movie = db.define('Movie', model);
var movies = [
    {title: 'Benher', description: 'Histry',Language:'English'},
    {title: 'Titanic', description: 'Romance',Language:'English'},
    {title: 'Rambo', description: 'Action',Language:'English'},

];

db.automigrate(undefined,function() {
    console.log('Created Table !!');
    Movie.create(movies, function (err, h) {
        if(err){
            console.log(err);
        } else {
            console.log("Inserted :", h);
            Movie.find({where: {title: 'Rambo'}}, function (err, arr) {
                console.log("Find' : \n", arr);
                var idval=arr[0].id;
                Movie.update({title: 'Rambo',id:parseFloat(idval),description:'Action'}, {Language: 'Marathi'}, function(err, data){

                    if(err){
                        console.log("error:"+err.message);
                    }

                    //Show All
                    Movie.all(function(err, data){
                        if(err){
                            console.log("error:"+err.message);
                        }
                        else {
                            console.log("\nData after updated :",data);
                        }
                        Movie.destroyAll({title: 'Rambo'}, function(err, data){

                            Movie.all(function(err, data){
                                console.log(data);
                            });
                        });

                    });
                });
            });
        }
    });
});
console.log(db);