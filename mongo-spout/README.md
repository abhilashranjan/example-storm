# Mongo Spout

This spout will listen mongo Db and emits the objects.
Capped Collections and Tailable Cursors
Capped Collections are fixed sized collections, that work in a way similar to circular buffers: once a collection fills its allocated space,it makes room for new documents by overwriting the oldest documents.

 MongoDB Capped Collections can be queried using Tailable Cursors, that are similar to the unix tail -f command.  Your application continue to retrieve documents as they are inserted into the collection. I also like to call this a "continuous query".

##Mongo DB Part
use users
@ switched to db users
 db.login.drop()
 db.createCollection('login',{capped:true, size:3})
@Created login collection with capped 
 db.login.insert({"username":"abc"})
 db.login.insert({"username":"abc1"})
 db.login.insert({"username":"abc2"})
