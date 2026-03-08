// Demo analytics events for Pipeline Agent
// Provides realistic event data for MongoDB → PostgreSQL pipeline demos

db = db.getSiblingDB('demo_analytics');

// Create events collection with sample web analytics data
var events = [];
var eventTypes = ['page_view', 'click', 'add_to_cart', 'purchase', 'signup', 'search'];
var pages = ['/home', '/products', '/products/detail', '/cart', '/checkout', '/account', '/search', '/about'];
var browsers = ['Chrome', 'Firefox', 'Safari', 'Edge'];
var devices = ['desktop', 'mobile', 'tablet'];
var countries = ['US', 'CA', 'GB', 'DE', 'FR', 'JP', 'AU', 'BR'];
var campaigns = [null, 'google_ads_q4', 'facebook_winter', 'email_newsletter', 'partner_ref'];

// Generate 200 events spread over the last 90 days
var now = new Date();
for (var i = 0; i < 200; i++) {
    var daysAgo = Math.floor(Math.random() * 90);
    var hoursAgo = Math.floor(Math.random() * 24);
    var eventTime = new Date(now.getTime() - (daysAgo * 86400000) - (hoursAgo * 3600000));
    var userId = 'user_' + (Math.floor(Math.random() * 50) + 1);
    var eventType = eventTypes[Math.floor(Math.random() * eventTypes.length)];
    var page = pages[Math.floor(Math.random() * pages.length)];

    var event = {
        event_id: 'evt_' + (10000 + i),
        event_type: eventType,
        user_id: userId,
        session_id: 'sess_' + Math.floor(Math.random() * 500),
        page_url: page,
        timestamp: eventTime,
        browser: browsers[Math.floor(Math.random() * browsers.length)],
        device: devices[Math.floor(Math.random() * devices.length)],
        country: countries[Math.floor(Math.random() * countries.length)],
        campaign: campaigns[Math.floor(Math.random() * campaigns.length)],
        properties: {}
    };

    // Add type-specific properties
    if (eventType === 'page_view') {
        event.properties.duration_seconds = Math.floor(Math.random() * 300) + 5;
        event.properties.scroll_depth = Math.floor(Math.random() * 100);
    } else if (eventType === 'click') {
        event.properties.element_id = 'btn_' + Math.floor(Math.random() * 20);
        event.properties.element_text = ['Buy Now', 'Learn More', 'Add to Cart', 'Sign Up'][Math.floor(Math.random() * 4)];
    } else if (eventType === 'add_to_cart') {
        event.properties.product_sku = 'SKU-' + String(Math.floor(Math.random() * 20) + 1).padStart(3, '0');
        event.properties.quantity = Math.floor(Math.random() * 3) + 1;
        event.properties.price = (Math.floor(Math.random() * 50000) + 999) / 100;
    } else if (eventType === 'purchase') {
        event.properties.order_id = 'ORD-' + (10000 + Math.floor(Math.random() * 30) + 1);
        event.properties.amount = (Math.floor(Math.random() * 80000) + 2000) / 100;
        event.properties.items_count = Math.floor(Math.random() * 5) + 1;
    } else if (eventType === 'search') {
        event.properties.query = ['wireless mouse', 'keyboard', 'monitor', 'headphones', 'desk'][Math.floor(Math.random() * 5)];
        event.properties.results_count = Math.floor(Math.random() * 50);
    }

    events.push(event);
}

db.events.insertMany(events);

// Create an index on timestamp for efficient incremental extraction
db.events.createIndex({ timestamp: 1 });
db.events.createIndex({ event_type: 1 });

print('Seeded ' + events.length + ' events into demo_analytics.events');
