from django.conf.urls.defaults import patterns, url

urlpatterns = patterns('tweets.views',
    url('^/?$', 'timeline', name='timeline'),
)