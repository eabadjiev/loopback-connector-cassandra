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
    'Language': {'type': String}
};
var Movie = db.define('Movie', model);
var movies = [
    {title: 'Benher', description: 'Histry',Language:'English'},
    {title: 'Titanic', description: 'Romance',Language:'English'},
    {title: 'Rambo', description: 'Action',Language:'English'},

];

db.isActual('Movie',function(err, actual) {
    if (!actual) {
        // Update Model
        db.autoupdate(function(){
            // CREATE
            Movie.create(heroes, function(err, h){
                if(err){
                    console.log(err);
                } else {
                    console.log("Created: \n", h);
                    // FIND
                    Movie.find({where: {universe: 'DC'}}, function(err, arr){
                        console.log("\nFind by universe 'DC' : \n", arr);
                        // UPDATE
                        Movie.update({universe: 'DC'}, {universe: 'DC Comics'}, function(err, data){
                            console.log("\nData after updating universe:");
                            //Show All
                            Movie.all(function(err, data){
                                console.log(data);
                                console.log("\nData after deleting a row:");
                                // DELETE
                                Movie.destroyAll({name: 'Aquaman'}, function(err, data){
                                    //Show All after delete
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