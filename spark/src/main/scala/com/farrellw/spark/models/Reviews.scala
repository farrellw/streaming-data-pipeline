package com.farrellw.spark.models

case class WrappedReview(js: Review)
case class Review(marketplace: String, customer_id: String, review_id: String, product_id: String, product_parent: Int, product_title: String, product_category: String, star_rating: Int, helpful_votes: Int, total_votes: Int, vine: String, verified_purchase: String, review_headline: String, review_body: String, review_date: java.sql.Timestamp)
case class EnrichedReview(review: Review, customer: Customer)
case class Customer(name: String, birthdate: String, mail: String, sex: String, username: String)
