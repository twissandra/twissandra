import uuid

from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponseRedirect
from django.core.urlresolvers import reverse

from tweets.forms import TweetForm

from lib.database import save_tweet, get_timeline

def timeline(request):
    form = TweetForm(request.POST or None)
    if request.user['is_authenticated'] and form.is_valid():
        tweet_id = str(uuid.uuid1())
        save_tweet(tweet_id, request.user['id'], {
            'id': tweet_id,
            'user_id': request.user['id'],
            'body': form.cleaned_data['body'],
        })
        return HttpResponseRedirect(reverse('timeline'))
    tweets = get_timeline(request.user['id'])
    context = {
        'form': form,
        'tweets': tweets,
    }
    return render_to_response('tweets/timeline.html', context,
        context_instance=RequestContext(request))