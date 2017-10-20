/**
 * Copyright 2017 Fondazione Ugo Bordoni
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.fub.bigdataplatform.connectors.twitter.utils;

import org.json.simple.JSONObject;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;

import com.mongodb.DBObject;

public class StatusConverter {

	private boolean includeEntities;

	public StatusConverter(boolean includeEntities){
		this.includeEntities = includeEntities;
	}
	
	public JSONObject toJSON(Status status) {
		JSONObject tweet = new JSONObject();
		extractTweetInfo(tweet, status);
		extractUserInfo(tweet, status);
		extractGeolocation(tweet, status);
		extractPlaceInfo(tweet, status);
		extractEntities(tweet, status);
		extractRetweetStatus(tweet, status);
		return tweet;
	}

	private void extractRetweetStatus(JSONObject tweet, Status status) {
		Status retweetStatus = status.getRetweetedStatus();

		if (retweetStatus!=null){

			JSONObject retweet = toJSON(retweetStatus);
			tweet.put("retweeted_status", retweet);		
		}
	}

	private void extractTweetInfo(JSONObject tweet, Status status) {

		tweet.put("id", status.getId()); // long
		tweet.put("text", status.getText());
		tweet.put("created_at", status.getCreatedAt().toString());
		tweet.put("retweet_count", status.getRetweetCount());
		tweet.put("favorited", status.isFavorited());
		tweet.put("favourited_count", status.getFavoriteCount());

		tweet.put("in_reply_to_status_id", status.getInReplyToStatusId());
		tweet.put("in_reply_to_user_id", status.getInReplyToUserId());
		tweet.put("in_reply_to_screen_name",
				status.getInReplyToScreenName());
		tweet.put("source", status.getSource());

		tweet.put("possibly_sensitive", status.isPossiblySensitive());
		tweet.put("truncated", status.isTruncated());

	}

	private void extractUserInfo(JSONObject tweet, Status status) {

		User user = (User) status.getUser();

		if (user != null) {

			JSONObject userObj = new JSONObject();

			/* GENERAL INFO */
			userObj.put("id", user.getId());
			userObj.put("name", user.getName());
			userObj.put("screen_name", user.getScreenName());
			userObj.put("description", user.getDescription());
			userObj.put("lang", user.getLang());
			userObj.put("created_at", user.getCreatedAt().toString());
			userObj.put("verified", user.isVerified());
			userObj.put("url", user.getURL());

			/* SOCIAL NETWORK INFO */
			userObj.put("followers_count", user.getFollowersCount());
			userObj.put("friends_count", user.getFriendsCount());
			userObj.put("statuses_count", user.getStatusesCount());
			userObj.put("listed_count", user.getListedCount());
			userObj.put("favourites_count", user.getFavouritesCount());

			/* LOCATION INFO */
			userObj.put("location", user.getLocation());
			userObj.put("time_zone", user.getTimeZone());
			userObj.put("utc_offset", user.getUtcOffset());
			userObj.put("geo_enabled", user.isGeoEnabled());

			tweet.put("user", userObj);

		}

	}

	private void extractGeolocation(JSONObject tweet, Status status) {

		GeoLocation geolocation = status.getGeoLocation();

		if (geolocation != null) {
			tweet.put("geolocation", status.getGeoLocation().getLatitude()
					+ "," + status.getGeoLocation().getLongitude());
		}

	}

	private void extractPlaceInfo(JSONObject tweet, Status status) {

		Place place = status.getPlace();

		if (place != null) {
			JSONObject placeObj = new JSONObject();
			placeObj.put("country", place.getCountry());
			placeObj.put("country_code", place.getCountryCode());
			placeObj.put("full_name", place.getFullName());
			placeObj.put("name", place.getName());
			placeObj.put("place_type", place.getPlaceType());
			placeObj.put("id", place.getId());
			placeObj.put("url", place.getURL());
			tweet.put("place", placeObj);
		}

	}

	private void extractEntities(JSONObject tweet, Status status) {
		
		if (includeEntities) {

			/* HASHTAG ENTITIES */
			HashtagEntity[] hashtagEntities = status.getHashtagEntities();

			if (hashtagEntities != null) {

				String hashtagEntitiesString = "";

				for (int i = 0; i < hashtagEntities.length; i++) {
					hashtagEntitiesString += (hashtagEntities[i].getText() + "|");
				}

				if (hashtagEntitiesString.length() > 0) {
					hashtagEntitiesString = hashtagEntitiesString.substring(0,
							hashtagEntitiesString.length() - 1);
					tweet.put("hashtagEntities", hashtagEntitiesString);
				}
			}

			/* URL ENTITIES */
			URLEntity[] urlEntities = status.getURLEntities();

			if (urlEntities != null) {
				String urlEntitiesString = "";
				for (int i = 0; i < urlEntities.length; i++) {
					urlEntitiesString += (urlEntities[i].getExpandedURL() + "|");
				}
				if (urlEntitiesString.length() > 0) {
					urlEntitiesString = urlEntitiesString.substring(0,
							urlEntitiesString.length() - 1);
					tweet.put("urlEntities", urlEntitiesString);
				}
			}

			/* USER MENTION ENTITIES */
			UserMentionEntity[] userMentionEntities = status
					.getUserMentionEntities();
			if (userMentionEntities != null) {
				String userMentionEntitiesString = "";
				for (int i = 0; i < userMentionEntities.length; i++) {
					userMentionEntitiesString += (userMentionEntities[i]
							.getScreenName() + "|");
				}
				if (userMentionEntitiesString.length() > 0) {
					userMentionEntitiesString = userMentionEntitiesString
							.substring(0,
									userMentionEntitiesString.length() - 1);
					tweet.put("userMentionEntities",
							userMentionEntitiesString);
				}
			}
		}

	}

}
