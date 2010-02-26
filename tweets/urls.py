from django.conf.urls.defaults import patterns, url

urlpatterns = patterns('tweets.views',
    url(r'^/?$', 'timeline', name='timeline'),
    url(r'^public/$', 'publicline', name='publicline'),
    url(r'^(?P<username>\w+)/$', 'userline', name='userline'),
)