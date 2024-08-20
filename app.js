const {MongoClient} = require('mongodb');
const mongodb_url = require('./db_url')

const fs = require('fs');
// const { count } = require('console');


const dbname = 'bank';
const collection_name = 'transactions';



async function main() {
    const client = new MongoClient(mongodb_url)
    try{
        await client.connect();
        console.log('connected to MongoDB');
        const transactionsCollection = client.db(dbname).collection(collection_name)

        const transactions =  JSON.parse(fs.readFileSync('transactions.json', 'utf-8'));
        await transactionsCollection.insertMany(transactions)
        console.log('inserted transactions into the collection');


        const pipeline = [
            {
                $group:{
                    _id: '$account_type',
                    totalAmount: {$sum: '$amount'},
                    avgAmount: {$avg: '$amount'},
                    count: {$sum: 1}
                },
            },

            {
                $sort:{totalAmount: -1}
            },

            {
                $project: {
                    _id:1,
                    account_type: '$_id',
                    totalAmount: 1,
                    avgAmount: 1,
                    count: 1
                }
            }

        ];

        const results = await transactionsCollection.aggregate(pipeline).toArray()
        console.log("Aggregation output")
        console.log(results)


    }catch(err){
        console.error("Error Message", err)
    }finally{
        await client.close();
    }
}

main().catch(console.dir);