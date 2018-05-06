/*
 Copyright Google Inc. 2018
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

exports.http_function = (request, response) => {

  // Imports the Cloud Datastore client library
  const Datastore = require('@google-cloud/datastore');

  // Your Google Cloud Platform project ID
  const projectId = process.env.GCLOUD_PROJECT;

  // Creates a client
  const datastore = new Datastore({
    projectId: projectId,
  });

  // Define the query: select the most recent record
  const query = datastore
    .createQuery('TrendingHashtags')
    .order('datetime', {
      descending: true,
    })
    .limit(1);

  // Execute the query
	datastore.runQuery(query).then(results => {
    var responseString = '<html><head><meta http-equiv="refresh" content="5"></head><body>';
    const entities = results[0];

    if (entities.length && entities[0].hashtags.length) {
      responseString += '<h1>Trending hashtags</h1>';
      responseString += '<ol>';
      entities[0].hashtags.forEach(hashtag => {
        responseString += `<li>${hashtag.name} (${hashtag.occurrences})`;
      });
      response.send(responseString);
    }
    else {
      responseString += 'No trending hashtags at this time... Try again later.';
      response.send(responseString);
    }
  })
  .catch(err => {
    response.send(`Error: ${err}`);
  });
};