from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponseRedirect

from users.forms import LoginForm, RegistrationForm

import cass

def login(request):
    login_form = LoginForm()
    register_form = RegistrationForm()
    next = request.REQUEST.get('next')
    if 'kind' in request.POST:
        if request.POST['kind'] == 'login':
            login_form = LoginForm(request.POST)
            if login_form.is_valid():
                uname = login_form.get_uname()
                request.session['uname'] = uname
                if next:
                    return HttpResponseRedirect(next)
                return HttpResponseRedirect('/')
        elif request.POST['kind'] == 'register':
            register_form = RegistrationForm(request.POST)
            if register_form.is_valid():
                username = register_form.save()
                request.session['uname'] = username
                if next:
                    return HttpResponseRedirect(next)
                return HttpResponseRedirect('/')
    context = {
        'login_form': login_form,
        'register_form': register_form,
        'next': next,
    }
    return render_to_response('users/login.html', context,
        context_instance=RequestContext(request))

def logout(request):
    request.session.pop('uname', None)
    return render_to_response('users/logout.html', {},
        context_instance=RequestContext(request))

def find_friends(request):
    friend_unames = []
    if request.user['is_authenticated']:
        friend_unames = cass.get_friend_unames(request.session['uname']) + [request.session['uname']]
    q = request.GET.get('q')
    result = None
    searched = False
    if q is not None:
        searched = True
        try:
            result = cass.get_user_by_username(q)
            result['friend'] = q in friend_unames
        except cass.DatabaseError:
            pass
    context = {
        'q': q,
        'result': result,
        'searched': searched,
        'friend_unames': friend_unames,
    }
    return render_to_response('users/add_friends.html', context,
        context_instance=RequestContext(request))

def modify_friend(request):
    next = request.REQUEST.get('next')
    added = False
    removed = False
    if request.user['is_authenticated']:
        if 'add-friend' in request.POST:
            cass.add_friends(request.session['uname'], [request.POST['add-friend']])
            added = True
        if 'remove-friend' in request.POST:
            cass.remove_friends(request.session['uname'], [request.POST['remove-friend']])
            removed = True
    if next:
        return HttpResponseRedirect(next)
    context = {
        'added': added,
        'removed': removed,
    }
    return render_to_response('users/modify_friend.html', context,
        context_instance=RequestContext(request))
