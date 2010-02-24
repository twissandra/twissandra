from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponseRedirect

from users.forms import LoginForm, RegistrationForm

def login(request):
    login_form = LoginForm()
    register_form = RegistrationForm()
    if 'kind' in request.POST:
        if request.POST['kind'] == 'login':
            login_form = LoginForm(request.POST)
            if login_form.is_valid():
                user_id = login_form.get_user_id()
                request.session['user_id'] = user_id
                if 'next' in request.REQUEST:
                    return HttpResponseRedirect(request.REQUEST['next'])
                return HttpResponseRedirect('/')
        elif request.POST['kind'] == 'register':
            register_form = RegistrationForm(request.POST)
            if register_form.is_valid():
                user_id = register_form.save()
                request.session['user_id'] = user_id
                if 'next' in request.REQUEST:
                    return HttpResponseRedirect(request.REQUEST['next'])
                return HttpResponseRedirect('/')
    context = {
        'login_form': login_form,
        'register_form': register_form,
    }
    return render_to_response('users/login.html', context,
        context_instance=RequestContext(request))

def logout(request):
    request.session.pop('user_id', None)
    return render_to_response('users/logout.html', {},
        context_instance=RequestContext(request))