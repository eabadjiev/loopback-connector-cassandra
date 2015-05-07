/**
 * Created by Amit Ashtekar on 5/4/2015.
 */
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
    'Language1': {'type': String}
};
var Movie = db.define('Movie', model);
var movies = [
    {title: 'Benher', description: 'Histry',Language1:'English'},
    {title: 'Titanic', description: 'Romance',Language1:'English'},
    {title: 'Rambo', description: 'Action',Language1:'English'},

];

db.isActual('Movie',function(err, actual) {
    if (!actual) {
        // Update Model
        db.autoupdate(function(){
            // CREATE
            Movie.create(movies, function (err, h) {
                if(err){
                    console.log(err);
                } else {
                    console.log("Inserted :", h);
                    Movie.find({where: {title: 'Rambo'}}, function (err, arr) {
                        console.log("Find' : \n", arr);
                        var idval=arr[0].id;
                        Movie.update({title: 'Rambo',id:parseFloat(idval),description:'Action'}, {Language1: 'Marathi'}, function(err, data){

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
    } else {
        console.log('No change in model definition.');
    }
});