from django.conf.urls.defaults import patterns, url

urlpatterns = patterns('users.views',
    url('^login/$', 'login', name='login'),
    url('^logout/$', 'logout', name='logout'),
    url(r'^find-friends/$', 'find_friends', name='find_friends'),
)